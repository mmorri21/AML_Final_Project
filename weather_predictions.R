### ===================
### Import packages
### ===================

lapply(c("caret",
         "dtplyr",
         "plyr",
         "tidyverse",
         "pROC",
         "TTR",
         "data.table",
         "foreach",
         "doParallel"), library, character.only = T)



### ===================
### Define variables
### ===================

rolling_period <- 7 # days
target_variables <- c("TemperatureF", "PrecipitationIn", "Conditions")
other_attributes <- c("Dew.PointF", "Sea.Level.PressureIn", "Wind.SpeedMPH")
merge_columns <- c("UTC_Date", "Zip_Code")

# Define fields to perform rolling average on
# (get it? rolling fields...)
rolling_fields <- c("TemperatureF",
                    "Dew.PointF",
                    "Sea.Level.PressureIn",
                    "Wind.SpeedMPH",
                    "PrecipitationIn",
                    "Conditions")

# List of dates to use for dataset
#dateRange <- list(min_date = as.Date('2010-10-31'), max_date = as.Date('2016-10-31'))
dateRange <- list(min_date = as.Date('2016-10-01'), max_date = as.Date('2016-10-31'))
dates <- seq(dateRange$min_date, dateRange$max_date, "days")

# Airport locations found on wunderground.com for zip code of interest.
# On the website, go to historical data and see which airport is referenced.
zip_codes <- list(my_zip = c(60502, "KDPA"), southwest_zip = c(61350, "KVYS"))



### ====================
### Parallel Processing
### ====================

# Set number of cores to use for parallel processing
no_cores <- detectCores()
if(no_cores > 1){
   no_cores = no_cores - 1
}

# Initiate cluster
registerDoParallel(no_cores)



### ===================
### Get data
### ===================

# Scrapes wunderground for data
getData <- function(date, zip_code, airport){return(read.csv(paste("https://www.wunderground.com/history/airport/", airport, "/",
                                                                   format(date, "%Y"), "/", format(date, "%m"), "/", format(date, "%d"),
                                                                   "/DailyHistory.html?req_city=Aurora&req_state=IL&req_statename=Illinois&reqdb.zip=",
                                                                   toString(zip_code), "&reqdb.magic=1&reqdb.wmo=99999&format=1", sep = ""),
                                                             stringsAsFactors = FALSE))}

# Initialize empty dataframe to be populated
df <- data.frame()

# Web scrape for data in parallel
# Do this for zip code of interest (60502) and a zip code to the southwest
# because weather generally moves to the northest in the northern hemisphere.
for (z in zip_codes){
  data <- foreach(dte = dates) %dopar% getData(dte, z[1], z[2])

  # Combine all of the dates into one dataframe
  data <- rbindlist(data)
  data$Zip_Code <- z[1]
  
  # Append to master dataframe
  df <- rbind(df, data)}
  
# Cleanup
rm(data, dateRange)



### ===================
### Preprocess data
### ===================

# Convert df to dataframe
df <- as.data.frame(df)

# Codify conditions vector
conditions <- c("Unknown",
                "Clear",
                "Partly Cloudy",
                "Scattered Clouds",
                "Overcast",
                "Mostly Cloudy",
                "Shallow Fog",
                "Fog",
                "Haze",
                "Mist",
                "Light Drizzle",
                "Drizzle",
                "Light Rain",
                "Rain",
                "Heavy Rain",
                "Light Thunderstorms and Rain",
                "Thunderstorm",
                "Thunderstorms and Rain",
                "Heavy Thunderstorms and Rain")

condition_weights <- 0:(length(conditions) - 1)
df$Conditions <- as.numeric(mapvalues(df$Conditions, from = conditions, to = condition_weights))

# Rename columns
names(df)[names(df) == "DateUTC.br..."] <- "UTC_Date"

# Replace "N/A" values of precipitation with 0.
df$PrecipitationIn[df$PrecipitationIn == "N/A"] <- 0

# Replace "Calm" wind values with 0.
df$Wind.SpeedMPH[df$Wind.SpeedMPH == "Calm"] <- 0

# Correct other columns to appropriate formats
df$Wind.SpeedMPH <- as.numeric(df$Wind.SpeedMPH)
df$PrecipitationIn <- as.numeric(df$PrecipitationIn)

# Remove outliers
assign("df", df[apply(df[, rolling_fields], 1, function(row){all(abs(row) != 9999)}),])

# Convert dates and times to correct format
df$UTC_DateTime <- as.POSIXct(df$UTC_Date)
df$UTC_Date <- as.Date(df$UTC_Date)

### Get time elapsed between each reading
  df$time_diff <- rbind(diff(as.matrix(df[,"UTC_DateTime"])), 0)
  
  # Reset time differences between zip codes by partitioning
  dt <- data.table(df[, c(merge_columns, "UTC_DateTime")])
  df$valRank <- dt[, valRank:=rank(UTC_DateTime), by = c("Zip_Code")]$valRank
  df$diff_rank <- rbind(diff(as.matrix(df[, "valRank"])), 0)
  df$time_diff[df$diff_rank != 1] <- 60 * (as.POSIXct(paste(df$UTC_Date[df$diff_rank != 1], "23:59:59 CDT")) - df$UTC_DateTime[df$diff_rank != 1])
  
rm(dt) # cleanup
  
# Drop unecessary columns
keeps <- c("TemperatureF",
           "Dew.PointF",
           "Sea.Level.PressureIn",
           "Wind.SpeedMPH",
           "PrecipitationIn",
           "Conditions",
           "WindDirDegrees",
           "UTC_Date",
           "UTC_DateTime",
           "Zip_Code",
           "time_diff")
df <- df[, keeps]

# Multiply out measures to be aggregated
for(r in rolling_fields){
  df[, paste(r, "_weighted", sep = "")] <- df[, r] * df[, "time_diff"]
}

# Get aggregate measures for each day using time based weight averages
df_agg <-
  df %>%
  group_by(UTC_Date, Zip_Code) %>%
  summarise_at(.cols = vars(ends_with("weighted")), .funs = mean)

df_time <-
  df %>% 
  group_by(UTC_Date, Zip_Code) %>%
  summarise(total_time = mean(time_diff, na.rm = TRUE))

# Join dataframe
df_agg <- merge(df_agg, df_time, by = merge_columns)
rm(df_time)

# Get weighted average aggregate measures
for(r in rolling_fields){
  column_name <- paste(r, "_weighted", sep = "")                                                    # determine column name
  df_agg[, column_name] <- as.numeric(df_agg[, column_name] / df_agg[, "total_time"])               # divide multiplied out value by total time for that day
  names(df_agg)[names(df_agg) == column_name] <- r                                                  # rename new column to appropriate metric
}

# Keep only necessary columns
df_agg <- df_agg[, -which(names(df_agg) %in% c("total_time"))]

rm(df, r, column_name) # cleanup

### Aggregate data ###
#
# Target variables: today's temperature and precipitation amount
#
# Features: 1) yesterday's weather attributes for this zip code and zip code to the southwest
#           2) previous week's weather attributes for this zip code and zip code to the southwest
#

# Function pivots fields out of dataframes with various date periods
pivot <- function(data, column, period){
  df_temp <- cast(data[, c(merge_columns, column)], UTC_Date ~ Zip_Code, value = column)            # pivot fields out
  columns <- mapply(c, zip_codes)[1, ]                                                              # get names of columns to rename
  names(df_temp)[names(df_temp) %in% columns] <- paste(columns, "_", column, "_", period, sep = "") # rename columns
  return(df_temp)
}


### TODAY ###
df_today <- df_agg[df_agg$UTC_Date > dates[rolling_period], c(merge_columns, target_variables)]     # create dataframe filtered to appropriate dates
df_today <- foreach(column = target_variables,
                    .packages = "reshape",
                    .combine = cbind) %dopar% pivot(df_today, column, "today")                      # pivot out rows to columns
df_today <- df_today[, !duplicated(names(df_today))]                                                # remove duplicate columns
df_today <- df_today[, -grep(zip_codes$southwest_zip[1], names(df_today))]                          # only keep target variables for today's records


### YESTERDAY ###
df_yesterday <-
  df_agg %>%
  filter(dates[rolling_period - 1] < UTC_Date & UTC_Date < tail(dates, 1))                          # create dataframe filtered to appropriate dates

df_yesterday$UTC_Date <- (df_yesterday$UTC_Date) + 1                                                # shift dates up one day
df_yesterday <- foreach(column = c(target_variables, other_attributes),
                       .packages = "reshape",
                       .combine = cbind) %dopar% pivot(df_yesterday, column, "yesterday")           # pivot out rows to columns
df_yesterday <- df_yesterday[, !duplicated(names(df_yesterday))]                                    # remove duplicate columns



### PREVIOUS WEEK ###
# Create function to get rolling averages for previous days
previous_week <- function (data, zip_code){
  df_temp <- data %>% filter(UTC_Date < tail(dates, 1), Zip_Code == zip_code)                       # create temporary dataframe filtereted to appropriate dates
  df_temp <- as.data.frame(cbind(df_temp[, merge_columns],
                                 apply(df_temp[, rolling_fields], 2, SMA, n = rolling_period)))     # create rolling averages of metrics
  
  return(df_temp)
}


df_previous_week <- foreach(zip_code = zip_codes,
                            .packages = c("dplyr", "TTR"),
                            .combine = rbind) %dopar% previous_week(df_agg, zip_code[1])            # create dataframe filtered to approprate dates

df_previous_week <- foreach(column = c(target_variables, other_attributes),
                           .packages = "reshape",
                           .combine = cbind) %dopar% pivot(df_previous_week,
                                                           column,
                                                           "previous_week")                         # pivot out rows to columns
df_previous_week <- df_previous_week[, !duplicated(names(df_previous_week))]                        # remove duplicate columns



# CREATE FINAL INPUT DATASET WITH ALL ATTRIBUTES
df <- merge(df_today, df_yesterday, by = "UTC_Date")
df <- merge(df, df_previous_week, by = "UTC_Date")

# Cleanup
rm(df_agg, df_yesterday, df_today, df_previous_week)

###########################
# Classify "Conditions" columns
columns <- grep("Conditions", names(df))

conditions_classify <- function(column){
  return (mapvalues(round(column), from = condition_weights, to = conditions))
}

foreach(column = columns, .packages = "plyr") %dopar% assign(df[, column] <- conditions_classify(df[, column]))
#df$Conditions <- mapvalues(round(df$Conditions), from = condition_weights, to = conditions)

### ===================
### Split into
### training, tuning,
### and test sets
### ===================

### ===================
### Train Models
### ===================

### ===================
### Tune Models
### ===================

### ===================
### Evaluate Models
### ===================

# Stop cluster
stopImplicitCluster()
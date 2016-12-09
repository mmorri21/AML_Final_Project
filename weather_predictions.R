### ===================
### Import packages
### ===================

lapply(c("caret",
         "dtplyr",
         "plyr",
         "tidyverse",
         "pROC",
         "ranger",
         "kernlab",
         "e1071",
         "TTR",
         "foreach",
         "zoo",
         "doParallel"), library, character.only = T)



### ===================
### Define variables
### ===================

rolling_period <- 7 # days                                                                          # number of days prior to date of prediction
                                                                                                    #   to calculate rolling averages for and
                                                                                                    #   use as predictor
target_variables <- c("TemperatureF", "PrecipitationIn", "Conditions")                              # values to predict
other_attributes <- c("Dew.PointF", "Sea.Level.PressureIn", "Wind.SpeedMPH", "Humidity")            # variables to use as predictors
merge_columns <- c("UTC_Date", "Zip_Code")                                                          # columns to be used in merging dataframes and grouping by
precipitation_buckets <- 5                                                                          # number of buckets for precipitation output to create

# Define fields to perform rolling average on
# (get it? rolling fields...)
rolling_fields <- c("TemperatureF",                                                                 # metrics to calculate rolling averages on
                    "Dew.PointF",
                    "Sea.Level.PressureIn",
                    "Wind.SpeedMPH",
                    "PrecipitationIn",
                    "Conditions",
                    "Humidity")

# List of dates to use for dataset
dateRange <- list(min_date = as.Date('2010-10-31'), max_date = as.Date('2016-10-31'))               # range of dates to collect data for
dates <- seq(dateRange$min_date, dateRange$max_date, "days")

# Airport locations found on wunderground.com for zip code of interest.
# On the website, go to historical data and see which airport is referenced.
zip_codes <- list(my_zip = c(60502, "KDPA"), southwest_zip = c(61350, "KVYS"))                      # zip codes to collect data for

# Define types of models to be used
regression_models <- c("glm", "svmLinear", "ranger")
classification_models <- c("J48", "lssvmLinear")

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

# Function that scrapes wunderground for data
getData <- function(date, zip_code, airport){
  data <- read.csv(paste("https://www.wunderground.com/history/airport/", airport, "/",
                          format(date, "%Y"), "/", format(date, "%m"), "/", format(date, "%d"),
                          "/DailyHistory.html?req_city=Aurora&req_state=IL&req_statename=Illinois&reqdb.zip=",
                          toString(zip_code), "&reqdb.magic=1&reqdb.wmo=99999&format=1", sep = ""),
                    stringsAsFactors = FALSE)
  
  if(nrow(data) <= 1){                                                                              # only keep records if data existed for that day
    data <- data.frame(matrix(ncol = 14, nrow = 0))
    }
  return(data)
  
  }

# Initialize empty dataframe to be populated
df <- data.frame()

# Web scrape for data in parallel
# Do this for zip code of interest (60502) and a zip code to the southwest
# because weather generally moves to the northest in the northern hemisphere.
for (z in zip_codes){
  data <- foreach(dte = dates) %dopar% getData(dte, z[1], z[2])                                     # get data from web

  # Combine all of the dates into one dataframe
  data <- rbindlist(data)                                                                           # combine all data returned from cores
  data$Zip_Code <- z[1]                                                                             # append zip code to data
  
  # Append to master dataframe
  df <- rbind(df, data)}                                                                            # come data from each zip code


rm(data, dateRange) # cleanup



### ===================
### Preprocess data
### ===================

# Convert df to dataframe
df <- as.data.frame(df)

# Drop duplicates
df <- df[!duplicated(df), ]

# Define unique conditions based on wunderground historical data
conditions <- c("Unknown",
                "Clear",
                "Partly Cloudy",
                "Scattered Clouds",
                "Overcast",
                "Mostly Cloudy",
                "Shallow Fog",
                "Patches of Fog",
                "Light Freezing Fog",
                "Fog",
                "Haze",
                "Mist",
                "Light Drizzle",
                "Drizzle",
                "Heavy Drizzle",
                "Light Rain",
                "Rain",
                "Heavy Rain",
                "Light Thunderstorms and Rain",
                "Thunderstorm",
                "Thunderstorms and Rain",
                "Heavy Thunderstorms and Rain",
                "Light Thunderstorms and Snow",
                "Thunderstorms and Snow",                
                "Heavy Thunderstorms and Snow",
                "Squalls",
                "Light Snow",
                "Snow",
                "Blowing Snow",
                "Heavy Snow",
                "Light Freezing Rain",
                "Light Freezing Drizzle",
                "Freezing Rain",
                "Ice Pellets")

conditions_replace <- c("Unknown",
                        "Clear",
                        "Partly Cloudy",
                        "Mostly Cloudy",
                        "Mostly Cloudy",
                        "Mostly Cloudy",
                        "Fog",
                        "Fog",
                        "Fog",
                        "Fog",
                        "Fog",
                        "Light Rain/Drizzle",
                        "Light Rain/Drizzle",
                        "Light Rain/Drizzle",
                        "Light Rain/Drizzle",
                        "Light Rain/Drizzle",
                        "Rainy",
                        "Rainy",
                        "Thunderstorms/Heavy Rain",
                        "Thunderstorms/Heavy Rain",
                        "Thunderstorms/Heavy Rain",
                        "Thunderstorms/Heavy Rain",
                        "Thunderstorms/Heavy Rain",
                        "Thunderstorms/Heavy Rain",
                        "Thunderstorms/Heavy Rain",
                        "Thunderstorms/Heavy Rain",                        
                        "Snow",
                        "Snow",
                        "Snow",
                        "Snow",
                        "Freezing Rain/Sleet",
                        "Freezing Rain/Sleet",
                        "Freezing Rain/Sleet",
                        "Freezing Rain/Sleet"
                        )

df$Conditions <- mapvalues(df$Conditions, from = conditions, to = conditions_replace)               # convert conditions to higher level of aggregation
condition_weights <- 1:(length(unique(conditions_replace))) - 1                                     # define condition numeric variables
df$Conditions <- as.numeric(mapvalues(df$Conditions,                                                # codify conditions in dataframe
                                      from = unique(conditions_replace),
                                      to = condition_weights))

names(df)[names(df) == "DateUTC.br..."] <- "UTC_Date"                                               # rename columns

# Convert some factors to numeric
columns <- c("Wind.SpeedMPH", "PrecipitationIn", "Humidity")
df[columns] <- lapply(df[columns], function(x) as.numeric(as.character(x)))

df[is.na(df)] <- 0                                                                                  # replace NA values with 0

df <- df[apply(df[rolling_fields], 1, function(row){all(abs(row) != 9999)}),]                       # remove outliers

# Convert dates and times to correct format
df$UTC_DateTime <- as.POSIXct(df$UTC_Date, format = "%Y-%m-%d %H:%M:%S")                            # convert to POSIXct (timestamp)
df$UTC_Date <- as.Date(df$UTC_Date)                                                                 # convert to date

# Clean up rows that have truncated/shifted data
df$UTC_DateTime <- as.POSIXct(na.approx(df$UTC_DateTime), origin = "1970-01-01")                    # linearly inteprolate to fill in missing values

# Get time elapsed between each reading
df$time_diff <- rbind(diff(as.matrix(df$UTC_DateTime)), 0)
  
# Reset time differences between zip codes by partitioning
lapply(zip_codes, function(z) df$time_diff[tail(which(df$Zip_Code == z[1]), 1)] <<- 3600)
  
# Drop unecessary columns
keeps <- c("TemperatureF",
           "Dew.PointF",
           "Sea.Level.PressureIn",
           "Wind.SpeedMPH",
           "PrecipitationIn",
           "Conditions",
           "Humidity",
           "UTC_Date",
           "UTC_DateTime",
           "Zip_Code",
           "time_diff")
df <- df[keeps]                                                                                     # drop columns from dataframe

# Multiply out measures to be aggregated
df[rolling_fields] <- lapply(df[rolling_fields], function(x) x * df$time_diff)                      # create time weighted average metrics

# Get aggregate measures for each day using time based weight averages
df_agg <-
  df %>%
  group_by(UTC_Date, Zip_Code) %>%
  summarise_at(.cols = rolling_fields, .funs = mean)                                                # aggregate average metrics by date and zip code

df_time <-
  df %>% 
  group_by(UTC_Date, Zip_Code) %>%
  summarise(total_time = mean(time_diff, na.rm = TRUE))                                             # aggregate sum of time by date and zip code

# Join dataframe
df_agg <- merge(df_agg, df_time, by = merge_columns)
rm(df_time)

# Get weighted average aggregate measures
df_agg[rolling_fields] <- lapply(df_agg[rolling_fields],
                                 function(x) as.numeric(x / df_agg$total_time))                     # calculate weighted average metrics on daily basis

# Keep only necessary columns
df_agg <- df_agg[, -which(names(df_agg) %in% c("total_time"))]                                      # drop columns from dataframe

rm(df) # cleanup

### Aggregate data ###
#
# Target variables: today's temperature and precipitation amount
#
# Features: 1) yesterday's weather attributes for this zip code and zip code to the southwest
#           2) previous week's weather attributes for this zip code and zip code to the southwest
#

# Function pivots fields out of dataframes with various date periods
pivot <- function(data, column, period){
  df_temp <- cast(data[c(merge_columns, column)], UTC_Date ~ Zip_Code, value = column)              # pivot fields out
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
  df_temp <- as.data.frame(cbind(df_temp[merge_columns],
                                 apply(df_temp[rolling_fields], 2, SMA, n = rolling_period)))       # create rolling averages of metrics
  
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
df <- merge(df_today, df_yesterday, by = "UTC_Date")                                                # merge datasets together
df <- merge(df, df_previous_week, by = "UTC_Date")

# Cleanup
rm(df_agg, df_yesterday, df_today, df_previous_week)


# Classify "Conditions" columns                                                                     # uncodify conditions
condition_column <- paste(zip_codes$my_zip[1], "_Conditions_today", sep = "")
df[condition_column] <- mapvalues(round(df[, condition_column]),
                                  from = condition_weights,
                                  to = unique(conditions_replace))

# Bucket precipitation outcomes                                                                     # bucket precipitation outcomes
max_precip <- max(df[, paste(zip_codes$my_zip[1], "_PrecipitationIn_today", sep = "")])
buckets <- seq(0, max_precip, length.out = precipitation_buckets)
reverse_squared <- (precipitation_buckets:1)^2
lapply(1:precipitation_buckets, function(x) buckets[x] <<- buckets[x] / reverse_squared[x])

precipitation_column <- paste(zip_codes$my_zip[1], "_PrecipitationIn_today", sep = "")
new_column <- paste(zip_codes$my_zip[1], "_precipitation_bucket", "_today", sep = "") 
df[, precipitation_column] <- cut(df[, precipitation_column], breaks = buckets, labels = 1:4)
names(df)[names(df) == precipitation_column] <- new_column                                          # rename precipitation bucket column

df[, new_column] <- as.numeric(df[, new_column])
df[is.na(df[, new_column]), new_column] <- 0                                                        # replace NA values
df[, new_column] <- as.character(df[, new_column])                                                  # convert to character

# Add month in as an attribute                                                                      # use month as predictor
df$month <- as.numeric(format(df$UTC_Date, "%m"))

############# ====================================
############# BEGIN MODELING
############# ====================================

# There will be 3 separate models here:
#
# 1. Predict temperature
# 2. Predict precipitation
# 3. Predict conditions (clear, cloudy, etc.)
#
# A list will be used to keep the results of each model

# Initialize list
results <- data.frame()

target_variables <- c("TemperatureF", "precipitation_bucket", "Conditions")                         # redefine based on new column names

# Loop through each target variable
for(t in target_variables){
  target <- paste(zip_codes$my_zip[1], "_", t, "_today", sep = "")                                  # target variable name
  
  # Determine columns to not keep as features in model
  features_exclude <- c(1, unlist(lapply(target_variables, function(x){
    grep(paste(zip_codes$my_zip[1], "_", x, "_today", sep = ""),
         names(df))})))
  
  exclude_columns <- setdiff(features_exclude, grep(target, names(df)))                             # define which columns to remove from datasets
  
  ### =====================
  ### Split data into
  ### training & test sets
  ### =====================
  
  train_index <- createDataPartition(y = df[, target], p = 0.7, list = FALSE)                       # create index of training set
  
  train_set <- df[train_index, -exclude_columns]                                                    # create training set
  test_set <- df[-train_index, -exclude_columns]                                                    # create test set
  rm(train_index) # cleanup
  
  # Rename target variable
  names(train_set)[names(train_set) == target] <- "target"
  names(test_set)[names(test_set) == target] <- "target"
 
  ### ===================
  ### Train Models
  ### ===================
  
  # Determine if regression problem or classification problem
  if(is.numeric(train_set$target)){
    models <- regression_models
    output_type <- "raw"
    model_type <- "regression"
  } else{
    models <- classification_models
    output_type <- "class"
    model_type <- "classification"
  }
  
  # Run models
  for(m in models){
    model <- train(target ~ ., data = train_set, method = m)
    
    ### ===================
    ### Test Models
    ### ===================
    prediction <- predict(model, newdata = test_set, type = output_type)
    
    ### ===================
    ### Evaluate Models
    ### ===================
    if(model_type == "regresion"){
      evaluation_metrics <- postResample(prediction, test_set$target)
      results[m, "RMSE"] <- evaluation_metrics$RMSE
      results[m, "Rsquared"] <- evaluation_metrics$Rsquared
    } else{
      roc_curve <- roc(test_set$target, prediction)
    }
  }
  
}

# Stop cluster
stopImplicitCluster()
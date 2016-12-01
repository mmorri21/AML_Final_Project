### ===================
### Import packages
### ===================

lapply(c("caret",
         "dtplyr",
         "tidyverse",
         "pROC",
         "foreach",
         "doParallel"), library, character.only = T)

### ===================
### Define variables
### ===================

# Set number of cores to use for parallel processing
no_cores <- detectCores()
if(no_cores > 1){
   no_cores = no_cores - 1
}

zip_codes <- list(my_zip = 60502, southwest_zip = 61350)

# Initiate cluster
registerDoParallel(no_cores)

# List of dates to use for dataset
#dateRange <- list(min_date = as.Date('2010-10-31'), max_date = as.Date('2016-10-31'))
dateRange <- list(min_date = as.Date('2016-10-01'), max_date = as.Date('2016-10-31'))
dates <- seq(dateRange$min_date, dateRange$max_date, "days")

### ===================
### Get data
### ===================

getData <- function(date, zip_code){return(read.csv(paste("https://www.wunderground.com/history/airport/KDPA/",
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
  data <- foreach(dte = dates) %dopar% getData(dte, z)

  # Combine all of the dates into one dataframe
  data <- rbindlist(data)
  data$Zip_Code <- z
  
  # Append to master dataframe
  df <- rbind(df, data)}
  
# Cleanup
rm(data)


### ===================
### Preprocess data
### ===================

# Rename columns
names(df)[names(df) == "DateUTC.br..."] <- "UTC_Date"

# Replace "N/A" values of precipitation with 0.
df$PrecipitationIn[df$PrecipitationIn == "N/A"] <- 0

# Replace "Calm" wind values with 0.
df$Wind.SpeedMPH[df$Wind.SpeedMPH == "Calm"] <- 0

# Convert dates and times to correct format
df$UTC_Date <- as.POSIXct(df$UTC_Date)
df$UTC_Time <- format(df$UTC_Date, "%H:%M:%S")
df$UTC_Date <- as.Date(df$UTC_Date)

# Correct other columns to appropriate formats
df$Wind.SpeedMPH <- as.numeric(df$Wind.SpeedMPH)
df$PrecipitationIn <- as.numeric(df$PrecipitationIn)

# Drop unecessary columns
keeps <- c("TemperatureF",
           "Dew.PointF",
           "Sea.Level.PressureIn",
           "Wind.SpeedMPH",
           "PrecipitationIn",
           "Conditions",
           "WindDirDegrees",
           "UTC_Date",
           "Zip_Code")
df <- subset(df, select = keeps)

### Aggregate data ###
#
# Target variables: today's temperature and precipitation amount
#
# Features: 1) yesterday's weather attributes for this zip code and zip code to the southwest
#           2) previous week's weather attributes for this zip code and zip code to the southwest
#

df_today <- 
  df %>%
  filter(Zip_Code == zip_codes$my_zip, UTC_Date > dates[7]) %>%
  group_by(UTC_Date) %>%
  summarize(mean_temperature = mean(TemperatureF, na.rm = TRUE),
            total_precipitation = sum(PrecipitationIn, na.rm = TRUE))

df_yesterday <-
  df %>%
  filter(dates[6] < UTC_Date & UTC_Date < tail(dates, 1)) %>%
  group_by(Zip_Code, UTC_Date + 1) %>%
  summarize(mean_temperature = mean(TemperatureF, na.rm = TRUE))

### NEED TO ADD WEEKLY MEASUREMENTS AND AGGREGATE MEASURES.  PROBABLY
### ALSO NEED TO WEIGHT THESE AGGREGATE MEASURES (AVERAGES) BASED ON TIME
### RATHER THAN SIMPLE AVERAGES

df_previous_week <- 
  df %>%
  filter(UTC_Date < tail(dates, 1)) %>%
  group_by(Zip_Code, UTC_Date) %>%
  summarize(mean_temperature = mean(TemperatureF, na.rm = TRUE))

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
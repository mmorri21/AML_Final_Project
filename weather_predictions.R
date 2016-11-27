### ===================
### Import packages
### ===================

lapply(c("caret", "dtplyr", "foreach", "doParallel"), library, character.only = T)

### ===================
### Define variables
### ===================

# Set number of cores to use for parallel processing
no_cores <- detectCores()
if(no_cores > 1){
   no_cores = no_cores - 1
}

# Initiate cluster
registerDoParallel(no_cores)

# List of dates to use for dataset
#dates <- seq(as.Date('2010-01-01'), as.Date('2016-10-31'), "days")
dates <- seq(as.Date('2016-10-01'), as.Date('2016-10-31'), "days")

### ===================
### Get data
### ===================

# Web scrape for data in parallel
getData <- function(date){return(read.csv(paste("https://www.wunderground.com/history/airport/KDPA/", format(date, "%Y"), "/", format(date, "%m"), "/", format(date, "%d"), "/DailyHistory.html?req_city=Aurora&req_state=IL&req_statename=Illinois&reqdb.zip=60502&reqdb.magic=1&reqdb.wmo=99999&format=1", sep = ""),
                          stringsAsFactors = FALSE))}

data <- foreach(dte = dates) %dopar% getData(dte)

# Combine all of the dates into one dataframe
data <- rbindlist(data)

### ===================
### Preprocess data
### ===================

# Rename columns
names(data)[names(data) == "DateUTC.br..."] <- "UTC_Date"

# Replace "N/A" values of precipitation with 0.
data$PrecipitationIn[data$PrecipitationIn == "N/A"] <- 0.00

# Convert dates and times to correct format
data$UTC_Date <- as.POSIXct(data$UTC_Date)
data$UTC_Time <- format(data$UTC_Date, "%H:%M:%S")
data$UTC_Date <- as.Date(data$UTC_Date)

# Drop unecessary columns
keeps <- c("TemperatureF",
           "Dew.PointF",
           "Sea.Level.PressureIn",
           "Wind.SpeedMPH",
           "PrecipitationIn",
           "Conditions",
           "WindDirDegrees",
           "UTC_Date")
data <- subset(data, select = keeps)


# Aggregate certain columns

# Stop cluster
stopImplicitCluster()
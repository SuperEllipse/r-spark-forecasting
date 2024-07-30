library(lubridate)
library(sparklyr)
library(dplyr)
library(forecast)
library(arrow)
library(vctrs)


## Modify Spark executor template
## Define the file path
#file_path <- "/tmp/spark-executor.json"
##
### Read the contents of the file
#file_contents <- readLines(file_path)
##
### Replace the string
#file_contents <- gsub('"workingDir":"/tmp"', '"workingDir":"/usr/local"', file_contents)
##
### Write the modified contents back to the file
#writeLines(file_contents, file_path)

#######################
# Configurations
########################

config <- spark_config()
             
config <- spark_config()
config$spark.security.credentials.hiveserver2.enabled <- "false"
config$spark.datasource.hive.warehouse.read.via.llap <- "false"
config$spark.sql.hive.hwc.execution.mode <- "spark"
config$spark.sql.extensions <- "com.qubole.spark.hiveacid.HiveAcidAutoConvertExtension"
config$spark.kryo.registrator <- "com.qubole.spark.hiveacid.util.HiveAcidKyroRegistrator"
config$sparklyr.jars.default <- "/opt/spark/optional-lib/hive-warehouse-connector-assembly.jar"
#config$spark.yarn.access.hadoopFileSystems <- "s3a://go01-demo/datalake/warehouse/tablespace/managed/hive"

config$spark.dynamicAllocation.minExecutors = 1
config$spark.dynamicAllocation.maxExecutors = 8
config$spark.executor.cores = 4
config$spark.executor.memory = "8g"

config$spark.executorEnv.R_LIBS="/home/cdsw/.local/lib/R/4.3/library"
config$spark.executorEnv.R_LIBS_USER="/home/cdsw/.local/lib/R/4.3/library"
config$spark.executorEnv.R_LIBS_SITE="/opt/cmladdons/r/libs"



sc <- spark_connect(config = config)

local_path = "/home/cdsw/local_df.csv"

# read from local directory
filtered_new <- spark_read_csv(sc, "sales_data", 
                          path =  local_path, 
                          memory = TRUE, 
                         infer_schema = TRUE)

# DataFrame collected into R
local_data <- filtered_new %>% collect()

local_data$date <- as.Date(local_data$date, format = "%Y-%m-%d")


mvar_fcast_fx <- function(df, horizon = 18) {
  library(sparklyr)
  library(dplyr)
  library(forecast)
  library(lubridate)
  library(arrow)
  
  horizon = 18
  
  print("Starting function...")
  print(paste("Number of rows in partition:", nrow(df)))
  
  # Create time series objects for the targets
  target_series_sales <- ts(df$total_sales, frequency = 52)
  target_series_volume <- ts(df$total_volume_sold, frequency = 52)
  
  # Fit the ARIMA model with external regressors for total sales
  fit_sales <- auto.arima(target_series_sales)
  print("ARIMA model fitted for total sales.")
  
  # Fit the ARIMA model with external regressors for total volume sold
  fit_volume <- auto.arima(target_series_volume)
  print("ARIMA model fitted for total volume sold.")
  
  # Extract the last date from the series
  series <- df$date
  last_date <- as.Date(tail(series, 1), format = "%Y-%m-%d")
  print(paste("Last date in series:", last_date))

  if (is.na(last_date)) {
    stop("The last date is not valid.")
  }
  
  # Generate future dates (weekly)
  
  print('horizon is')
  print(horizon)
  
  #future_dates <- seq.Date(last_date + 7, by = "week", length.out = horizon)
  future_dates <-seq(last_date, by = "week", length.out = horizon)
  print("Future dates generated:")
  print(future_dates)
  
  
  # Forecasting future values
  forecast_sales <- forecast(fit_sales, h = horizon)
  forecast_volume <- forecast(fit_volume, h = horizon)
  
  print("Forecasts created.")
  
  forecast_df <- data.frame(
    Date = future_dates,
    forecast_volume = as.numeric(forecast_volume$mean),
    fc_sales = as.numeric(forecast_sales$mean)
  )
  
  print("Forecast dataframe created.")

  return(forecast_df)
}


#######################################################
# run with x store_number





# change the filtering to pick n categories and k stores  


cat_num = 3
store_num = 5

#sorted_data <- local_data %>%
#  arrange(desc(total_sales))
#
## Then, take a sample of the top 10 srl_num
#select_50 <- unique(sorted_data$srl_num)[1:50]
#
## Filter the data frame to include only the top 10 srl_num
#smaller_data <- local_data %>%
#  filter(srl_num %in% select_50)


#sorted_data <- local_data %>%
#  arrange(desc(total_sales))

yearly_summary <- local_data %>%
  mutate(year = year(date)) %>%  # Extract year from date
  group_by(year, category_name) %>%
  summarize(
    total_yearly_sales = sum(total_sales, na.rm = TRUE),
    total_yearly_volumes = sum(total_volume_sold, na.rm = TRUE)
  )


# filters for only last year

max_year = max(yearly_summmary$year)

cat_summary <- yearly_summary %>%
  filter(year == max_year) 

sorted_data <- cat_summary %>%
  arrange(desc(total_sales))

## Then, take a sample of the top 10 srl_num
top_categories <- unique(sorted_data$category_name)[1:cat_num]

## Filter the data frame to include only the top 10 srl_num
catagory_filter <- local_data %>%
  filter(catagory_name %in% top_categories)


# now do the same thing for stores 

yearly_summary_2 <- catagory_filter %>%
  mutate(year = year(date)) %>%  # Extract year from date
  group_by(year, store_number) %>%
  summarize(
    total_yearly_sales = sum(total_sales, na.rm = TRUE),
    total_yearly_volumes = sum(total_volume_sold, na.rm = TRUE)
  )

store_summary <- yearly_summary %>%
  filter(year == max_year) 

sorted_store_data <- store_summary %>%
  arrange(desc(total_sales))

## Then, take a sample of the top 10 srl_num
top_stores <- unique(sorted_store_data$store_number)[1:store_num]

## Filter the data frame to include only the top 10 srl_num
store_filter <- catagory_filter %>%
  filter(store_number %in% top_stores)


########### Spark Apply approach with n unique_id##########################
spark_df <- copy_to(sc, store_filter, "spark_df", overwrite = TRUE)

# Partition the DataFrame by unique_id
#spark_df_partitioned <- spark_df %>% 
#                        sdf_repartition(partition_by = "srl_num")
spark_df_partitioned <- spark_df %>% 
                        sdf_repartition(partition_by = c("store_number", "category_name", "srl_num"))


# Apply the function to each partition
execution_time <- system.time({
  forecast_results <- spark_df_partitioned %>% 
                      spark_apply(mvar_fcast_fx, group_by = c("store_number", "category_name", "srl_num"), packages = FALSE)
})

local_fc_results <- forecast_results %>% collect()

# add store number and category to forecast results

# Print the execution time
print('forecast took:')
print(execution_time)

## recored time for forecast

# for 10 forecasts
#[1] "forecast took:"
#   user  system elapsed 
#  0.312   0.028  19.828 

# for 50 forecasts
#   user  system elapsed 
#  0.300   0.000  52.585 

# save the forecast results 
write.csv(local_fc_results, "forecast_results.csv")


spark_disconnect(sc)
library(sparklyr)
library(dplyr)
library(lubridate)
library(purrr)
library(forecast)


# Spark configuration
config <- spark_config()
config$spark.security.credentials.hiveserver2.enabled <- "false"
config$spark.datasource.hive.warehouse.read.via.llap <- "false"
config$spark.sql.hive.hwc.execution.mode <- "spark"
config$spark.sql.extensions <- "com.qubole.spark.hiveacid.HiveAcidAutoConvertExtension"
config$spark.kryo.registrator <- "com.qubole.spark.hiveacid.util.HiveAcidKyroRegistrator"
config$sparklyr.jars.default <- "/opt/spark/optional-lib/hive-warehouse-connector-assembly.jar"
config$spark.yarn.access.hadoopFileSystems <- "s3a://go01-demo/datalake/warehouse/tablespace/managed/hive"


## Oliver's Configs

#config$spark.dynamicAllocation.minExecutors = 1
#config$spark.dynamicAllocation.maxExecutors = 8
#config$spark.executor.cores = 4
#config$spark.executor.memory = "8g"

##

# Connect to Spark
sc <- spark_connect(config = config)

s3_path <- file.path(Sys.getenv("DATASET_PATH"),Sys.getenv( "DATASET_SMALL_NAME"))

# Read the dataset from S3 into Spark
sales_tbl <- spark_read_csv(sc, name = "sales_data", path = s3_path, header = TRUE, infer_schema = TRUE)


# Rename columns to lowercase
sales_data <- sales_tbl %>%
  rename_with(tolower)

###########################################
# EDA - Get summary stats
distinct_stores <- sales_data %>%
  select(store_number) %>%
  distinct() %>%
  collect()

print(nrow(distinct_stores))

distinct_categories <- sales_data %>%
  select(category_name) %>%
  distinct() %>%
  collect()

print(nrow(distinct_categories))

###########################################

# pare down down data store, category_name, sales, volume_liters

aggregated_df <- sales_data %>%
  group_by(store_number, date, category_name) %>%
  summarize(
    total_sales = sum(sale_dollars),
    total_volume_sold = sum(volume_sold_liters),
    .groups = 'drop'
  )


# Collect the results back to R
aggregated_result <- collect(aggregated_df)

# Convert date column to Date format
aggregated_result$date <- as.Date(aggregated_result$date, format = "%m/%d/%Y")



# Create a new column for the week
aggregated_result <- aggregated_result %>%
  mutate(week = floor_date(date, unit = "week"))

# Aggregate the data by store, week, and category
weekly_aggregated_result <- aggregated_result %>%
  group_by(store_number, week, category_name) %>%
  summarize(
    total_sales = sum(total_sales, na.rm = TRUE),
    total_volume_sold = sum(total_volume_sold, na.rm = TRUE),
    .groups = 'drop'
  )


# Rename date_new to date
weekly_aggregated_result <- weekly_aggregated_result %>%
  rename(date = week)


# Check the max date in the entire dataset
max_date <- max(weekly_aggregated_result$date)

# Include only store/category comb that contain this last date
stores_categories_with_max_date <- weekly_aggregated_result %>%
  filter(date == max_date) %>%
  select(store_number, category_name) %>%
  distinct()

native_df <- weekly_aggregated_result %>%
  semi_join(stores_categories_with_max_date, by = c("store_number", "category_name"))

##########################

native_df <- native_df %>%
  group_by(store_number, category_name) %>%
  mutate(srl_num = cur_group_id()) %>%
  ungroup()

######################################################
#num_unique_srl_num <- native_df %>%
#  summarise(n = n_distinct(srl_num)) %>%
#  pull(n)
#print(paste("Number of unique srl_num:", num_unique_srl_num))
#
#num_unique_store_number <- native_df %>%
#  summarise(n = n_distinct(store_number)) %>%
#  pull(n)
#print(paste("Number of unique store_number:", num_unique_store_number))
#
#num_unique_category_name <- native_df %>%
#  summarise(n = n_distinct(category_name)) %>%
#  pull(n)
#print(paste("Number of unique category_name:", num_unique_category_name))
#
#num_rows <- nrow(native_df)
#print(paste("Number of rows in native_df:", num_rows))

#####################################################


## Check for date completeness and fill missing weeks with 0 sales
#complete_weeks <- seq.Date(min(native_df$date), max_date, by = "week")
#store_category_combinations <- native_df %>%
#  select(store_number, category_name) %>%
#  distinct()
#
#complete_data <- expand.grid(
#  store_number = store_category_combinations$store_number,
#  category_name = store_category_combinations$category_name,
#  date = complete_weeks
#)
#
#native_df <- complete_data %>%
#  left_join(native_df, by = c("store_number", "category_name", "date")) %>%
#  mutate(
#    total_sales = ifelse(is.na(total_sales), 0, total_sales),
#    total_volume_sold = ifelse(is.na(total_volume_sold), 0, total_volume_sold)
#  )

#vish:  
# important queries for EDA
# Lists number of rows per category
#native_df%>%group_by(category_name)%>%summarize(row_count = n())%>%arrange(desc(row_count))
# 
# Calculate yearly sales and volumes for each category
# yearly_summary <- native_df %>%
#   mutate(year = year(date)) %>%  # Extract year from date
#   group_by(year, category_name) %>%
#   summarize(
#     total_yearly_sales = sum(total_sales, na.rm = TRUE),
#     total_yearly_volumes = sum(total_volume_sold, na.rm = TRUE)
#   )
# 
# # Show the result
# yearly_summary %>% collect() %>% print()


# Print number of rows in native_df
print(nrow(native_df))

# Write to CSV without row names
write.csv(native_df, "local_df.csv", row.names = FALSE)

# Disconnect from Spark
spark_disconnect(sc)
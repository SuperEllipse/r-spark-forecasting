#This shows the Data Ingest and EDA of the R pipeline. 
# check the data_ingest_and_EDA.Rmd to see the actual details of what the code functionality covers

library(sparklyr)
library(dplyr)
library(lubridate)

# Spark configuration
config <- spark_config()
config$spark.security.credentials.hiveserver2.enabled <- "false"
config$spark.datasource.hive.warehouse.read.via.llap <- "false"
config$spark.sql.hive.hwc.execution.mode <- "spark"
config$spark.sql.extensions <- "com.qubole.spark.hiveacid.HiveAcidAutoConvertExtension"
config$spark.kryo.registrator <- "com.qubole.spark.hiveacid.util.HiveAcidKyroRegistrator"
config$sparklyr.jars.default <- "/opt/spark/optional-lib/hive-warehouse-connector-assembly.jar"
config$spark.yarn.access.hadoopFileSystems <- "s3a://go01-demo/datalake/warehouse/tablespace/managed/hive"

# Connect to Spark
sc <- spark_connect(config = config)


#s3_path <- "s3a://go01-demo/data/iowa-liquor-sales/Iowa_Liquor_Sales.csv"
s3_path <- file.path(Sys.getenv("DATASET_PATH"),Sys.getenv( "DATASET_NAME"))


# Read the dataset from S3 into Spark
sales_tbl <- spark_read_csv(sc, name = "sales_data", path = s3_path, header = TRUE, infer_schema = TRUE)

# Rename columns to lowercase
sales_data <- sales_tbl %>%
  rename_with(tolower)

# This is our base data 
head(sales_data)

# let us to do some data transformation and 
# include the 
most_recent_date <- sales_data %>%
  summarize(max_date = max(to_date(date, "MM/dd/yyyy"))) %>%
  collect() %>%
  pull(max_date)

cutoff_date <- most_recent_date - years(3)

last_3_years_data <- sales_data %>%
  filter(to_date(date, "MM/dd/yyyy") >= cutoff_date) %>%
  mutate(
    date_parsed = to_date(date, "MM/dd/yyyy"),
    year = year(date_parsed),
    month = month(date_parsed),
    day_of_month = dayofmonth(date_parsed),
    week_number = weekofyear(date_parsed),
    day_of_week = dayofweek(date_parsed),
    quarter = quarter(date_parsed),
    is_weekend = (dayofweek(date_parsed) %in% c(1, 7))
  )


# creating a smaller dataset
dataset_small_path = file.path(Sys.getenv("DATASET_PATH"), Sys.getenv("DATASET_SMALL_NAME"))
spark_write_csv(last_3_years, path = dataset_small_path,  mode = "overwrite")



# --------------------------------------------
# IMPORTS AND SETUP
# --------------------------------------------
from pyspark.sql import SparkSession # might not need
from pyspark.sql.functions import *
from pyspark.sql.types import *
from utils import *
import os


# Set up logger
logger = config_logging("station")


# --------------------------------------------
# CONFIGURATION
# --------------------------------------------
tables = {
        "dim_station": "dim_station",
        "dim_time": "dim_time",
        "fact_station": "stg_station_status"
    }
coordinates = {
        "lat_min": -90, "lat_max": 90,
        "lon_min": -180, "lon_max": 180
    }


# --------------------------------------------
# SCHEMA DEFINITION
# --------------------------------------------
def get_station_schema():
    return StructType([
        StructField("id", StringType(), True),
        StructField("station_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("last_updated", StringType(), True),
        StructField("free_bikes", IntegerType(), True),
        StructField("empty_slots", IntegerType(), True),
        StructField("uid", StringType(), True),
        StructField("renting", IntegerType(), True),
        StructField("returning", IntegerType(), True),
        StructField("address", StringType(), True),
        StructField("has_ebikes", StringType(), True),
        StructField("ebikes", IntegerType(), True),
        StructField("normal_bikes", IntegerType(), True),
        StructField("number", IntegerType(), True),
        StructField("slots", IntegerType(), True),
        StructField("current_timestamp", StringType(), True)
    ])


# --------------------------------------------
# CLEANING FUNCTIONS
# --------------------------------------------
# function to clean bike station data
def clean_station_data(df):
    # recall constants
    lat_min, lat_max = coordinates["lat_min"], coordinates["lat_max"]
    lon_min, lon_max = coordinates["lon_min"], coordinates["lon_max"]
    
    # cleaning
    df = df.na.drop(subset=["station_id", "latitude", "longitude"])
    df = df.withColumn("last_updated_ts", to_timestamp("last_updated")) \
           .withColumn("ingested_at", to_timestamp("current_timestamp")) \
           .filter(col("latitude").between(lat_min, lat_max) & col("longitude").between(lon_min, lon_max)) \
           .dropDuplicates(["station_id", "last_updated_ts"]) \
           .withColumn("name", trim(col("name"))) \
           .withColumn("renting", col("renting").cast("boolean")) \
           .withColumn("returning", col("returning").cast("boolean")) \
           .withColumn("has_ebikes", col("has_ebikes").cast("boolean"))

    return df

# --------------------------------------------
# BUILD FACT & DIMENSION TABLES
# --------------------------------------------
# function to create data models
def create_star_schema_tables(station_df):
    dim_station = station_df.select(
        col("id").alias("network_id"),
        "station_id", col("name").alias("station_name"),
        "latitude", "longitude", "address", "uid",
        "has_ebikes", "number", "slots"
    )

    dim_time = station_df.select(to_timestamp("current_timestamp").alias("current_timestamp")) \
        .withColumn("timestamp_id", unix_timestamp("current_timestamp")) \
        .withColumn("date", date_format("current_timestamp", "yyyy-MM-dd")) \
        .withColumn("day", date_format("current_timestamp", "d").cast("int")) \
        .withColumn("month", date_format("current_timestamp", "M").cast("int")) \
        .withColumn("year", date_format("current_timestamp", "yyyy").cast("int")) \
        .withColumn("hour", date_format("current_timestamp", "H").cast("int")) \
        .withColumn("minute", date_format("current_timestamp", "m").cast("int")) \
        .withColumn("day_of_week", date_format("current_timestamp", "u").cast("int")) \
        .dropDuplicates(["timestamp_id"])

    fact_station = station_df.select(
        "station_id", col("id").alias("network_id"),
        unix_timestamp(to_timestamp("current_timestamp")).alias("timestamp_id"),
        "free_bikes", "empty_slots", "renting",
        "returning", "ebikes", "normal_bikes", "slots"
    )

    return dim_station, dim_time, fact_station


# --------------------------------------------
# DATABASE WRITE
# --------------------------------------------
def write_to_postgres(df, table_name, mode="overwrite"):
    try:
        df.write.jdbc(
            url = config["jdbc_url"],
            table = table_name,
            mode = mode,
            properties = config["db_properties"]
        )
        logger.info(f"Successfully wrote to {table_name}")
    except Exception as e:
        logger.error(f"Failed to write {table_name}: {str(e)}", exc_info = True)


# --------------------------------------------
# ORCHESTRATION
# --------------------------------------------
# function to orchestrate
def main():
    try:
        logger.info("Loading secrets.")
        eventhub_connstr = load_secrets()

        # run 
        logger.info("Reading from Event Hub.")
        station_schema = get_station_schema()
        parsed_df = read_eventhub_stream(eventhub_connstr, station_schema)

        # run cleaning function
        logger.info("Cleaning station and network data.")
        station_df = clean_station_data(parsed_df)

        # write clean cities to reference weather data
        logger.info("")
        if os.path.exists("reference_data/city_list.csv") == False:
            city_df = df.select("city").distinct()
            spark.write.csv(
            city_df, "reference_data/city_list.csv", header=True, mode="overwrite"
            )

        # run dimensional modelling function
        logger.info("Building schema.")
        dim_station, dim_time, fact_station = create_star_schema_tables(station_df)

        # run database write function
        logger.info("Writing to database.")
        write_to_postgres(dim_station, tables["dim_station"])
        write_to_postgres(dim_time, tables["dim_time"])
        write_to_postgres(fact_station, tables["fact_station"], mode = "append")

    except Exception as e:
        logger.critical("Pipeline failed", exc_info=True)


# --------------------------------------------
# ENTRY POINT
# --------------------------------------------
if __name__ == "__main__":
    main()

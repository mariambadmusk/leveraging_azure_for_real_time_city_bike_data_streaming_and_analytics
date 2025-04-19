from pyspark.sql.functions import col, date_format, trim, to_timestamp, expr
from pyspark.sql.types import BooleanType, StructType, StringType, DoubleType, IntegerType, StructField
from utils import config_logging



# Set up logger
logger = config_logging()



def get_station_schema():
    """ Get schema for bike station data """
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
        StructField("renting",  BooleanType(), True),
        StructField("returning",  BooleanType(), True),
        StructField("address", StringType(), True),
        StructField("has_ebikes", StringType(), True),
        StructField("ebikes", IntegerType(), True),
        StructField("normal_bikes", IntegerType(), True),
        StructField("number", IntegerType(), True),
        StructField("slots", IntegerType(), True),
        StructField("ingested_at", StringType(), True)
    ])


# function to clean bike station data
def clean_station_data(df):
    """ Clean the bike station data"""
    try:
        # set up constants
        lat_min, lat_max = -90, 90
        lon_min, lon_max = -180, 180
        
        # cleaning
        df = df.na.drop(subset=["station_id", "latitude", "longitude"])
        df = df.withColumn("last_updated", to_timestamp("last_updated")) \
            .filter(col("latitude").between(lat_min, lat_max) & col("longitude").between(lon_min, lon_max)) \
            .withColumn("name", trim(col("name"))) \
            .withColumn("renting", col("renting").cast("boolean")) \
            .withColumn("returning", col("returning").cast("boolean")) \
            .withColumn("has_ebikes", col("has_ebikes").cast("boolean"))
        
        df = df.withColumn("timestamp_id", expr("uuid()"))
        
        df = df.dropDuplicates(["station_id", "last_updated"]) 
        logger.info("Data cleaned successfully")

        return df

    except Exception as e:
        logger.error(f"Error cleaning data: {e}")
        return None

   


def create_star_schema_tables(station_df):
    """ Create star schema tables for bike stations """
    try:
        dim_station = station_df.select(
        "station_id",
            "network_id",  
        "station_name",
            "latitude", 
            "longitude", 
            "address",
            "uid",
            "has_ebikes", 
            "number",
            "renting",
            "returning",
            "slot" 
        )

        dim_time = station_df.select(to_timestamp("last_updated").alias("last_updated")) \
            .select("timestamp_id") \
            .withColumn("date", date_format("last_updated", "yyyy-MM-dd")) \
            .withColumn("day", date_format("last_updated", "d").cast("int")) \
            .withColumn("month", date_format("last_updated", "M").cast("int")) \
            .withColumn("year", date_format("last_updated", "yyyy").cast("int")) \
            .withColumn("hour", date_format("last_updated", "H").cast("int")) \
            .withColumn("minute", date_format("last_updated", "m").cast("int")) \
            .withColumn("day_of_week", date_format("last_updated", "u").cast("int")) \
            .dropDuplicates(["timestamp_id"]
                            )

        fact_station = station_df.select(
            "station_id", 
            "network_id",
            "timestamp_id",
            "free_bikes",
            "empty_slots",
            "ebikes", 
            "normal_bikes", 
            "slots"
            )

        return dim_station, dim_time, fact_station
    except Exception as e:
        logger.error(f"Error creating star schema tables: {e}")
        return None, None, None







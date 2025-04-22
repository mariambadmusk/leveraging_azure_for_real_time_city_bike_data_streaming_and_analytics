from pyspark.sql.functions import col, date_format, trim, to_timestamp, expr, explode
from pyspark.sql.types import BooleanType, StructType, StringType, DoubleType, IntegerType, StructField, ArrayType, LongType
from scripts.utils import config_logging



# Set up logger
logger = config_logging()

def get_station_schema():
    return StructType([
        StructField("stations", ArrayType(StructType([
            StructField("network_id", StringType(), True),
            StructField("station_id", StringType(), True),
            StructField("street", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("last_updated_tz", StringType(), True),
            StructField("free_bikes", IntegerType(), True),
            StructField("empty_slots", IntegerType(), True),
            StructField("uid", StringType(), True),
            StructField("is_renting", BooleanType(), True),
            StructField("is_returning", BooleanType(), True),
            StructField("address", StringType(), True),
            StructField("has_ebikes", StringType(), True),
            StructField("ebikes", IntegerType(), True),
            StructField("normal_bikes", IntegerType(), True),
            StructField("number", IntegerType(), True),
            StructField("slots", IntegerType(), True),
            StructField("ingested_at", StringType(), True)
        ])))
    ])



def get_station_schema():

    station_schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("free_bikes", IntegerType(), True),
        StructField("empty_slots", IntegerType(), True),
        StructField("extra", StructType([
            StructField("uid", StringType(), True),
            StructField("number", IntegerType(), True),
            StructField("renting", BooleanType(), True),
            StructField("returning", BooleanType(), True),
            StructField("last_updated", LongType(), True),
            StructField("address", StringType(), True),
            StructField("slots", IntegerType(), True),
            StructField("normal_bikes", IntegerType(), True),
            StructField("ebikes", IntegerType(), True),
            StructField("has_ebikes", BooleanType(), True),
        ]), True)
    ])

    network_schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("location", StructType([
            StructField("city", StringType(), True),
            StructField("country", StringType(), True)
        ]), True),
        StructField("company", ArrayType(StringType()), True),
        StructField("stations", ArrayType(station_schema), True)  
    ])

    main_schema = StructType([
        StructField("network", network_schema, True)
    ])

    return main_schema




def flatten_df(df):
    
    flattened_df = df.select(
            col("network.id").alias("network_id"),
            explode("network.stations").alias("station")
        ).select(
            "network_id",
            col("station.id").alias("station_id"),
            col("street").alias("station_name"),
            col("station.latitude").alias("latitude"),
            col("station.longitude").alias("longitude"),
            col("station.free_bikes").alias("free_bikes"),
            col("station.empty_slots").alias("empty_slots"),
            col("station.extra.uid").alias("uid"),
            col("station.extra.number").alias("number"),            
            col("station.extra.renting").alias("is_renting"),
            col("station.extra.returning").alias("is_returning"),
            col("station.extra.last_updated").alias("last_updated_tz"),
            col("station.extra.address").alias("address"),
            col("station.extra.slots").alias("slots"),
            col("station.extra.normal_bikes").alias("normal_bikes"),
            col("station.extra.ebikes").alias("ebikes"),
            col("station.extra.has_ebikes").alias("has_ebikes"),
        )       
    return flattened_df



def clean_station_data(df):
    """ Clean the bike station data"""
    try:
        # set up constants
        lat_min, lat_max = -90, 90
        lon_min, lon_max = -180, 180
        
        # cleaning
        df = df.na.drop(subset=["station_id", "latitude", "longitude"])
        df = df.withColumn("last_updated_tz", to_timestamp("last_updated_tz")) \
            .filter(col("latitude").between(lat_min, lat_max) & col("longitude").between(lon_min, lon_max)) \
            .withColumn("street", trim(col("street"))) \
            .withColumn("is_renting", col("is_renting").cast("boolean")) \
            .withColumn("is_returning", col("is_returning").cast("boolean")) \
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
        "street",
            "latitude", 
            "longitude", 
            "address",
            "uid",
            "has_ebikes", 
            "number",
            "is_renting",
            "is_returning",
            "slot" 
        )

        dim_time = station_df.select(to_timestamp("last_updated_tz").alias("last_updated_tz")) \
            .select("timestamp_id") \
            .withColumn("date", date_format("last_updated_tz", "yyyy-MM-dd")) \
            .withColumn("day_", date_format("last_updated_tz", "d").cast("int")) \
            .withColumn("month_", date_format("last_updated_tz", "M").cast("int")) \
            .withColumn("year_", date_format("last_updated_tz", "yyyy").cast("int")) \
            .withColumn("hour_", date_format("last_updated_tz", "H").cast("int")) \
            .withColumn("minute_", date_format("last_updated_tz", "m").cast("int")) \
            .dropDuplicates(["timestamp_id"])
            # .withColumn("day_of_week", date_format("last_updated_tz", "u").cast("int")) \


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







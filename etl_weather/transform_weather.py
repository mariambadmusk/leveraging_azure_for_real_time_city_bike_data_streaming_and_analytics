from utils import config_logging
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.dataframe import DataFrame

logger = config_logging()

def get_weather_schema():
    """ Transfroms schema for the weather data"""
    schema =  StructType([
    StructField("location", StructType([
        StructField("name", StringType(), True),
        StructField("country", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("localtime", StringType(), True),
    ]), True),
    StructField("current", StructType([
        StructField("last_updated", StringType(), True),
        StructField("temp_c", DoubleType(), True),
        StructField("wind_mph", DoubleType(), True),
        StructField("wind_degree", LongType(), True),
        StructField("wind_dir", StringType(), True),
        StructField("humidity", LongType(), True),
        StructField("cloud", LongType(), True),
        StructField("feelslike_c", DoubleType(), True),
        StructField("heatindex_c", DoubleType(), True),
        StructField("uv", DoubleType(), True),
    ]), True)
    ])

    return schema
    

def flatten_df(df):

    flat_df = df.select(
    col("location.name").alias("city"),
    col("location.country").alias("country"),
    col("location.lat").alias("weather_latitude"),
    col("location.lon").alias("weather_longtitude"),
    col("location.localtime").alias("localtime"), 
    col("current.last_updated").alias("last_updated_tz"),
    col("current.temp_c").alias("temp_c"),
    col("current.wind_mph").alias("wind_mph"),
    col("current.wind_degree").alias("wind_degree"),
    col("current.wind_dir").alias("wind_dir"),
    col("current.humidity").alias("humidity"),
    col("current.cloud").alias("cloud"),
    col("current.feelslike_c").alias("feelslike_c"),
    col("current.heatindex_c").alias("heatindex_c")
    )

    return flat_df


def clean_data(df: DataFrame)-> DataFrame:

    # stadardise last_updated and localtime column into timestamp
    df.dropDuplicates()

    df = df.withColumn("localtime_tz", to_timestamp(col("localtime"), "yyyy-MM-dd HH:mm"))

    df = df.withColumn("last_updated", to_timestamp(col("last_updated"), "yyyy-MM-dd HH:mm"))

    fact_weather_df = df.select("city", "country", "timezone", "localtime_tz", "last_updated_tz", "weather_latitude", "weather_longitude", "weather_condition", "temp_c", "feelslike_c", "heat_index", "wind_mph", "wind_degree", "wind_dir", "humidity", "cloud")


    return fact_weather_df


if __name__ == "__main__":
    pass
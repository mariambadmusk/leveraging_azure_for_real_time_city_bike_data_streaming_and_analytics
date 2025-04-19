from utils import *
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
import json
import aiohttp
import asyncio
import pandas as pd


logger = config_logging()


async def fetch(url: str):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                return data


async def fetch_all_weather(api_key: str, city_list: list, weather_event_hub_name, weather_event_hub_namespace) -> None:  

    try:                            
        tasks = []

        for city in city_list:
            url = f"https://api.weatherapi.com/v1/current.json?key={api_key}&q={city}"
            tasks.append(fetch(url))


        responses = await asyncio.gather(*tasks)


        for response in responses:
            if response:
                location_data = response.get("location", {})
                current = response.get("current", {})

                if location_data and current:
                    streaming_data = {
                        "timezone": location_data.get("tz_id"),
                        "country": location_data.get("country"),
                        "last_updated": current.get("last_updated"),
                        "localtime": location_data.get("localtime"),
                        "city": location_data.get("name"),
                        "condition": current.get("condition", {}).get("text"),
                        "temp_c": current.get("temp_c"),
                        "wind_mph": current.get("wind_mph"),
                        "wind_degree": current.get("wind_degree"),
                        "wind_dir": current.get("wind_dir"),
                        "humidity": current.get("humidity"),
                        "cloud": current.get("cloud"),
                        "feelslike_c": current.get("feelslike_c"),
                        "heat_index": current.get("heatindex_c"),
                    }

                    # Send data to event hub
                    await publish_to_eventhub(weather_event_hub_name, weather_event_hub_namespace, streaming_data)
                        
                                
                logger.info(f"Streaming data sent to Weather Event Hub")
                
    except aiohttp.ClientError as e:
        logger.error(f"Error fetching data: {e}")
        return None
    

def get_weather_schema():
    """ Transfroms schema for the weather data"""

    schema =  StructType([
        StructField("timezone", StringType(), True),
        StructField("country", StringType(), True),
        StructField("last_updated", StringType(), True),
        StructField("localtime", StringType(), True),
        StructField("city", StringType(), True),
        StructField("condition", StringType(), True),
        StructField("temp_c", FloatType(), True),
        StructField("wind_mph", FloatType(), True),
        StructField("wind_degree", FloatType(), True),
        StructField("wind_dir", StringType(), True),
        StructField("humidity", FloatType(), True),
        StructField("cloud", FloatType(), True),
        StructField("feelslike_c", FloatType(), True),
        StructField("heat_index", FloatType(), True),
        StructField("timestamp", TimestampType(), True),
        ])


    return schema


def clean_data(df: DataFrame)-> DataFrame:

    # stadardise last_updated and localtime column into timestamp
    df.dropDuplicates()

    df = df.withColumn("localtime", to_timestamp(col("localtime"), "yyyy-MM-dd HH:mm"))

    df = df.withColumn("last_updated", to_timestamp(col("last_updated"), "yyyy-MM-dd HH:mm"))

    fact_weather_df = df.select("city", "country", "timezone", "localtime", "last_updated", "condition", "temp_c", "feelslike_c", "heat_index", "wind_mph", "wind_degree", "wind_dir", "humidity", "cloud")

    return fact_weather_df


def weather_main():
    try:
        # Set up constants
        logger = config_logging()
        logger.info("Setting up constants for Weather etl...")

        app_name = "fetchAndConsumeWeatherApi" 
        mode = "append"      
        weather_schema = get_weather_schema()
        api_key = config["weather_api_key"]
        if not api_key:
            logger.error("Missing API key for weather data: check your configuration.")
            return
        
        # Set up Event Hub connection string
        weather_event_hub_connstr = load_secrets()
        weather_event_hub_name = config["eventhub_name"]
        weather_event_hub_namespace = config["eventhub_namespace"]


        # Intialise spark session
        spark = intialise_spark_session(app_name)

        if not spark or weather_event_hub_name or weather_event_hub_namespace or weather_event_hub_connstr:
            logger.error("Missing variable configuration: check your configuration.")
            return
        
        
        # read csv for weather query parameter
        city_df = pd.read_csv("reference_data/city_list.csv")
        city_list = city_df.to_list()

        
        # Read network IDs and fetch data from API
        logger.info("Starting to fetch data from API...")
        asyncio.run(fetch_all_weather(api_key, city_list, weather_event_hub_name, weather_event_hub_namespace))

        # Read from Kafka topic
        logger.info("Reading from Kafka topic...")
        streamed_df = read_eventhub_stream(spark, weather_event_hub_connstr, weather_schema)
        
    
        # Transform data
        logger.info("Transforming df and creating tables...")
        cleaned_df = clean_data(streamed_df)

        # Write to database
        logger.info("Writing to database...")
        write_to_postgres(cleaned_df, "fact_weather", mode)
        logger.info("Data written to weather table successfully.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        spark.stop() 
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    weather_main()




                
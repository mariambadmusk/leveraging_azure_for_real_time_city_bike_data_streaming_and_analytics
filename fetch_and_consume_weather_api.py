from utils import config_logging, configure_kafka_producer, intialise_spark_session, read_and_transform_kafka_stream , write_to_database
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, DataFrame
import logging
import json
import aiohttp
import asyncio
import os
import pandas as pd

logging = config_logging("extract_and_stream_weather")



async def fetch(url: str):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                return data


async def fetch_all_weather(api_key: str, city_list: list, producer: str, topic: str) -> None:  

    try:                            
        tasks = []

        for city in city_list:
            url = f"https://api.weatherapi.com/v1/current.json?key={api_key}&q={city}"
            tasks.append(fetch(url))


        responses = await asyncio.gather(*tasks)


        all_stations = []
                        
        if len(responses) == len(all_locations): 
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

                        producer.send(topic, value=json.dumps(streaming_data).encode('utf-8'))
                        producer.flush()
                                
                logging.info(f"Streaming data sent to Kafka topic {topic}")
                
    except aiohttp.ClientError as e:
        logging.error(f"Error fetching data: {e}")
        return None
    

def transform_schema():
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
        app_name = "fetchAndConsumeWeatherApi"
        bootstrap_servers = "KAFKA_BROKERS"
        topic = "weather-feed-stream"
        jbdc_url = os.getenv("JDBC_URL")
        schema = transform_schema()
        api_key = os.getenv("WEATHER_API_KEY")
        table = ""

        spark = intialise_spark_session(app_name)
        producer  = configure_kafka_producer(bootstrap_servers)

        if not spark or bootstrap_servers or producer or topic:
            logging.error("Missing variable configuration: check your configuration.")
            return
        
        city_df = pd.read_csv("reference_data/city_list.csv")
        city_list = city_df.tolist()

        asyncio.run(fetch_all_weather(api_key, city_list, producer, topic))
        
        transformed_df = read_and_transform_kafka_stream (spark, schema, bootstrap_servers, topic)

        cleaned_df = clean_data(transformed_df)

        write_to_database(cleaned_df, table, "append", jbdc_url)

    
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        producer.close()
        spark.stop()
        
        logging.info("Spark session stopped.")


if __name__ == "__main__":
    weather_main()


                
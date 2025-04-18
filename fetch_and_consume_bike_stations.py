from utils import config_logging, configure_kafka_producer, intialise_spark_session, write_to_database, read_and_transform_kafka_stream 
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
import os
import logging
import json
import asyncio
import aiohttp
import asyncio
import json
import os



logging = config_logging("extract_and_stream_stations")



def read_network_ids():
    """ Read bike network ids from CSV file """

    try:
        with open("bike_networks.csv", "r") as f:
            data = csv.reader(f)
            all_ids = list(data)

            logging.info("Network IDs read successfully.")
            return all_ids
    except Exception as e:
        logging.error(f"Error reading file: {e}")
        return None



async def fetch(url: str) -> dict:
    """ Fetch data from API asynchronously """

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.text()
                    return data
                logging.info("Data fetched successfully.")
    except aiohttp.ClientError as e:
        logging.error(f"Error fetching data: {e}")
        return None
    

    
async def fetch_all_stations(producer: str, topic: str) -> None:

    """ Fetch all bike networks asynchronously """

    try:
        all_ids = read_network_ids()
        
        url = "http://api.citybik.es/v2/networks"
        urls = [f"{url}/{network_id}" for network_id in all_ids]

        tasks = []
        for url in urls:
            tasks.append(fetch(url))    
            

        responses = await asyncio.gather(*tasks)


        for response in responses:
            if response:
                response = json.loads(response)  # convert text (str) to json to extract the networks

            tasks_stations = response.get("network", {}).get("stations", [])
            

            for stations in tasks_stations:
                streaming_data = ({
                    "network_id": response.get("network", {}).get("id"),
                    "station_id": stations["id"],
                    "name": stations["name"],
                    "latitude": stations["latitude"],
                    "longitude": stations["longitude"],
                    "last_updated": stations["timestamp"],  # rename timestamp key to last_updated
                    "free_bikes": stations["free_bikes"],
                    "empty_slots": stations["empty_slots"],
                    "uid": stations["extra"].get("uid", None),
                    "renting": stations["extra"].get("renting", 0),
                    "returning": stations["extra"].get("returning", 0),
                    "address": stations["extra"].get("address", None),
                    "has_ebikes": stations["extra"].get("has_ebikes", 0),
                    "ebikes": stations["extra"].get("ebikes", 0),
                    "normal_bikes": stations["extra"].get("normal_bikes", 0),
                    "number": stations["extra"].get("number", 0),
                    "slots": stations["extra"].get("slots", 0),
                })
                
                producer.send(topic, value=json.dumps(streaming_data).encode('utf-8'))
                producer.flush()
                        
        logging.info(f"Streaming data sent to Kafka topic {topic}")
        
    except aiohttp.ClientError as e:
        logging.error(f"Error fetching data: {e}")
        return None



def transform_schema():

    """ Transform schema for bike networks """

    schema = StructType([
        StructField("network_id", StringType(), True),
        StructField("station_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True),
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
        StructField("slots", IntegerType(), True)
    ])
    
    return schema



    
   
def station_main():
    try:
        app_name = "fetchAndConsumeStationsApi"
        bootstrap_servers = "KAFKA_BROKERS"
        topic = "stations-feed-stream"
        schema = transform_schema()
        jbdc_url = os.getenv("JDBC_URL")


        spark = intialise_spark_session(app_name)
        producer  = configure_kafka_producer(bootstrap_servers)
        schema = transform_schema()

        if not spark or bootstrap_servers or producer or topic:
            logging.error("Missing variable configuration: check your configuration.")
            return

        asyncio.run(fetch_all_stations(producer, topic))
        
        transformed_df = read_and_transform_kafka_stream(spark, schema, bootstrap_servers, topic)

        write_to_database(transformed_df, "stations", "append", jbdc_url)



    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        producer.close()
        spark.stop()
        
        logging.info("Spark session stopped.")


if __name__ == "__main__":
    station_main()



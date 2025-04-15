from utils import config_logging, configure_kafka_producer, intialise_spark_session, write_to_db
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import requests
import logging
import json


logging = config_logging()


def extract_bike_networks(producer: str, topic: str) -> None:

    url = f"http://api.citybik.es/v2/networks"

    try:
        
        response = requests.get(url)

        if response.status_code == 200:
            logging.info("Data fetched successfully.")
            data = response.json()
            
            all_ids = []
            for network in data["networks"]:
                if network["location"]["country"] in ["US", "GB", "CA"]:

                    network_location = network.get("location", {})

                    streaming_data =  {
                    "id": network.get("id"),
                    "name": network.get("name"),
                    "latitude": network_location.get("latitude"),
                    "longitude": network_location.get("longitude"),
                    "city": network_location.get("city"),
                    "country": network_location.get("country"),
                    "company": network.get("company")[0]
                    }

                    all_ids.append(network.get("id"))
                
                    producer.send(topic, value=json.dumps(streaming_data).encode('utf-8'))
                    producer.flush()
                    
                    logging.info(f"Streaming data sent to Kafka topic {topic}")

                    return all_ids

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data: {e}")
        return None


def transform_schema():
    bike_networks = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("company", StringType(), True),
    ])

    return bike_networks


def consume_kafka_stream(spark, bootstrap_servers, topic):
    try:
        df = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribe", topic) \
            .option("delimiter", ",")\
            .load()
        logging.info("Data consumed successfully.")

        transformed_df = df.selectExpr("CAST(value AS STRING)")\
        .select(from_json(col("value", transform_schema)\
        .alias("data")))\
        .select("data.*")

        return transformed_df

    except Exception as e:
        logging.error(f"Error consuming data: {e}")
        return None
    
   
def main():
    try:
        spark = intialise_spark_session()
        bootstrap_servers = "KAFKA_BROKERS"
        producer  = configure_kafka_producer(bootstrap_servers)
        topic = "list-of-bike-networks"

        if not spark or bootstrap_servers or producer or topic:
            logging.error("Missing variable configuration: check your configuration.")
            return

        all_ids = extract_bike_networks(producer, topic)

        spark.write.csv(all_ids, "bike_networks.csv", header=True)

        transformed_df = consume_kafka_stream(spark, bootstrap_servers, topic)

        # Write to database
        write_to_db(transformed_df, "bike_networks", "overwrite")


    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        producer.close()
        spark.stop()
        
        logging.info("Spark session stopped.")


if __name__ == "__main__":
    main()



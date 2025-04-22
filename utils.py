import logging
from pyspark.sql import SparkSession 
from pyspark.sql.functions import from_json, col
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
import json


logger = logging.getLogger(__name__)

def config_logging():
    global logger

    logger.setLevel(logging.DEBUG)
    
    # remove after azure configuration
    handler = logging.FileHandler("app.log")

    # edit for Azure configuration
    # handler = logging.StreamHandler()

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


def intialise_spark_session(app_name):
    """ Intialise Spark Session 
    
    Returns:
        spark (SparkSession): Spark Session
    """
    spark = SparkSession.builder\
                .appName(app_name)\
                .getOrCreate()
    logger.info(f"Spark session {app_name} created successfully")
    return spark



async def publish_to_eventhub(connection_str, event_hub_name, data):
    producer = EventHubProducerClient.from_connection_string(conn_str=connection_str, eventhub_name=event_hub_name)

    async with producer:
        event_data_batch = await producer.create_batch()

        for each_data in data:
            try:
                event_data_batch.add(EventData(body = each_data))
                logger.info("Batch sent successfully.")
            except ValueError:
                logger.info("Batch full, creating and sending new batch.")
                await producer.send_batch(event_data_batch)
                event_data_batch = await producer.create_batch()
                event_data_batch.add(EventData(json.dumps(each_data)))
                logger.info("Batch sent successfully.")
            except Exception as e:
                logger.error(f"Error sending to Event Hub: {e}")
                


def read_eventhub_stream(spark, connection_string, schema):
    try:
        # encrypt Event Hubs configuration
        sc = spark.sparkContext

        eh_conf = {
        'eventhubs.connectionString': sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string),
        'eventhubs.startingPosition': 'latest'
                }
        
        # read from Event Hub
        raw_df = spark.readStream \
            .format("eventhubs") \
            .options(**eh_conf) \
            .load()
        
        # convert body field (contains incoming stream data in binary format) from binary to string (readability)
        json_df = raw_df.selectExpr("cast(body as string) AS json")
        parsed_df = json_df.select(from_json(col("json"), schema).alias("data")) 
        logger.info("df parsed successfully")
        return parsed_df
   
    except Exception as e:
        logger.error(f"parsing df failed: {e}")



def write_to_database(jdbc_url, properties, df, table_name, mode):
    """ Write DataFrame to Azure PostgreSQL database """
    try:
        df.write.jdbc(
            url = jdbc_url,
            table = table_name,
            mode = mode,
            properties = properties
        )
        logger.info(f"Successfully wrote to {table_name}")
    except Exception as e:
        logger.error(f"Failed to write {table_name}: {e}", exc_info=True)



def read_from_database(jdbc_url, properties, spark, table_name):
    try:
        df = spark.read.jdbc(
            url=jdbc_url,
            table=table_name,
            properties=properties
        )
        logger.info(f"Successfully read from {table_name}")
        return df
    except Exception as e:
        logger.error(f"Failed to write {table_name}: {e}", exc_info=True)
        return None

if __name__ == "__main__":
    pass

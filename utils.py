import logging
from pyspark.sql import SparkSession 
from pyspark.sql.functions import from_json, col
from configparser import ConfigParser
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
from azure.identity.aio import DefaultAzureCredential


# Load configuration
config = ConfigParser()
config.read(path)

credential = DefaultAzureCredential()
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

    try:
        spark = SparkSession.builder()\
                .appName(app_name)\
                .getOrCreate()
        logger.info(f"Spark session {app_name} created successfully")
        return spark
    
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")
        raise



async def publish_to_eventhub(event_hub_name, event_hub_namespace, data):

    # Create a producer client to send messages to the event hub.
    # Specify a credential that has correct role assigned to access
    # event hubs namespace and the event hub name.

    credential = DefaultAzureCredential()
    
    producer = EventHubProducerClient(
        fully_qualified_namespace=event_hub_namespace,
        eventhub_name=event_hub_name,
        credential=credential,
    )



    async with producer:
        # Create a batch.
        event_data_batch = await producer.create_batch()

        # Add events to the batch.
        event_data_batch.add(EventData(str(data).encode('utf-8'))) 

        # Send the batch of events to the event hub.
        await producer.send_batch(event_data_batch)

        # Close credential when no longer needed.
        await credential.close()


def load_secrets():
    config["db_properties"]["password"] = dbutils.secrets.get(scope= config["eventhub_scope"], key= config["pg_password_key"])
    
    return dbutils.secrets.get(scope=config["eventhub_scope"], key= config["eventhub_key"])



def read_eventhub_stream(spark, connection_string, schema):
    try:
        # encrypt Event Hubs configuration
        eh_conf = {
        'eventhubs.connectionString': spark._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string)
    }
        # read from Event Hub
        raw_df = spark.readStream.format("eventhubs")\
            .options(**eh_conf).load()
        
        # convert body field (contains incoming stream data in binary format) from binary to string (readability)
        json_df = raw_df.selectExpr("cast(body as string) as json")
        parsed_df = json_df.select(from_json(col("json"), schema).alias("data")).select("data.*")

        logger.info("df parsed successfully")
        return parsed_df
    
    except Exception as e:
        logger.error(f"parsing df failed: {e}")



def write_to_postgres(df, table_name, mode):
    """ Write DataFrame to Azure PostgreSQL database """
    try:
        df.write.jdbc(
            url = config["jdbc_url"],
            table = table_name,
            mode = mode,
            properties = config["db_properties"]
        )
        logger.info(f"Successfully wrote to {table_name}")
    except Exception as e:
        logger.error(f"Failed to write {table_name}: {e}", exc_info=True)
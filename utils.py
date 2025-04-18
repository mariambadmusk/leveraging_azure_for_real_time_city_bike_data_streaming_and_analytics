import json
import logging
from pyspark.sql import SparkSession # might not need
from pyspark.sql.functions import *
from pyspark.sql.types import *

# --------------------------------------------
# LOGGING
# --------------------------------------------
def config_logging(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # remove after azure configuration
    handler = logging.FileHandler("app.log")

    # edit for Azure configuration
    # handler = logging.StreamHandler()

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger

# --------------------------------------------
# SPARK SESSION
# --------------------------------------------
def intialise_spark_session(app_name):
        """ Intialise Spark Session 
    
        Returns:
            spark (SparkSession): Spark Session
        """

        try:
            spark = SparkSession.builder()\
                    .appName(app_name)\
                    .getOrCreate()
            if spark:
                logging.info("Spark session created successfully.")
                return spark
        
        except Exception as e:
            logging.error(f"Error creating Spark session: {e}")
            raise

# --------------------------------------------
# CONFIGURATION
# --------------------------------------------
config = {
    "eventhub_scope": "secret_keys",
    "eventhub_key": "eventhub_connstr",
    "pg_password_key": "pg_password",
    "jdbc_url": "<your-jdbc-url>",              # TODO: replace
    "db_properties": {
        "user": "<your-username>",              # TODO: replace
        "password": None,                       # TODO as well
        "driver": "org.postgresql.Driver"
    }}

# --------------------------------------------
# SECRET HANDLING
# --------------------------------------------
def load_secrets():
    config["db_properties"]["password"] = dbutils.secrets.get(scope= config["eventhub_scope"], key= config["pg_password_key"])
    return dbutils.secrets.get(scope=config["eventhub_scope"], key= config["eventhub_key"])

# --------------------------------------------
# READ FROM EVENT HUB
# --------------------------------------------
# function to read and parse incoming stream
def read_eventhub_stream(connection_string, schema):
    try:
        # set Event Hubs configuration
        eh_conf = {
        'eventhubs.connectionString': sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string)
    }
        # read from Event Hub
        raw_df = spark.readStream.format("eventhubs").options(**eh_conf).load()
        # convert body field (contains incoming stream data in binary format) from binary to string (readability)
        json_df = raw_df.selectExpr("cast(body as string) as json")
        parsed_df = json_df.select(from_json(col("json"), schema).alias("data")).select("data.*") 
        logger.info("df parsed successfully")
        return parsed_df
    
    except Exception as e:
        logger.error(f"parsing df failed: {e}")
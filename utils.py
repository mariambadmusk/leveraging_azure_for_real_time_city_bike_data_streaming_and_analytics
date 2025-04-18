from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col
from kafka import KafkaProducer
import logging
import os

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
    
       
        
def configure_kafka_producer(bootstrap_servers):
    """ Configure Kafka Producer
    
    Args:
        boostrap_servers (str): Kafka bootsrap Servers

    Returns:
        producer (KafkaProducer): Kafka Producer
        
    """

    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: x.encode('utf-8')
        )
    except Exception as e:
        logging.error(f"Error configuring Kafka producer: {e}")
        raise
    
    return producer



def read_and_transform_kafka_stream (spark, schema, bootstrap_servers, topic):
    try:
        df = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribe", topic) \
            .option("delimiter", ",")\
            .option("startingOffsets", "earliest") \
            .load()
        
        logging.info("Data consumed successfully.")

        transformed_df = df.selectExpr("CAST(value AS STRING)")\
        .select(from_json(col("value", schema)\
        .alias("data")))\
        .select("data.*")

        logging.info("Data schema transformed to dataframe successfully.")

        return transformed_df

    except Exception as e:
        logging.error(f"Error consuming data: {e}")
        return None



def write_to_database(df: DataFrame, table_name: str , mode: str, jbdc_url: str) -> None:
    """ Write to database"""

    try:
        df.write\
           .format("com.databricks.spark.sqldw")\
           .option("url", jbdc_url\
           .option("dbtable", table_name)\
           .mode(mode)\
           .save()
        )
        logging.info(f"Data written to {table_name} in {mode} mode.")
        
    except Exception as e:
        logging.error(f"Error writing to database: {e}")
        raise
            
           

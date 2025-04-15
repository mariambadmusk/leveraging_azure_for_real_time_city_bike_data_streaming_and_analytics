from pyspark.sql import SparkSession, DataFrame
from kafka import KafkaProducer
import logging
import os

def config_logging():


def intialise_spark_session():
        """ Intialise Spark Session 
    
        Returns:
            spark (SparkSession): Spark Session
        """
        spark = SparkSession.builder()\
                .appName("CityBike ETL")\
                .getOrCreate()
    
        return spark
        
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



def write_to_db(df: DataFrame, table_name: str , mode: str) -> None:
    """ Write to database"""

    try:
        df.write\
           .format("com.databricks.spark.sqldw")\
           .option("url", os.getenv("JBDC_URL")\
           .option("dbtable", table_name)\
           .mode(mode)\
           .save()
        )
        logging.info(f"Data written to {table_name} in {mode} mode.")
        
    except Exception as e:
        logging.error(f"Error writing to database: {e}")
        raise
            
           

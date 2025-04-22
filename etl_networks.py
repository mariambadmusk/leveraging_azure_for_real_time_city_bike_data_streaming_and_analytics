
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import requests
import logging
import sys
import os
import pandas as pd
sys.path.append(os.path.abspath("scripts"))
from utils import config_logging, intialise_spark_session, write_to_database


logging = config_logging()



def extract_bike_networks(url) -> None:
    try:
        response = requests.get(url)

        if response.status_code == 200:
            logging.info("Data fetched successfully.")
            data = response.json()
            
            return data
        else:
            logging.error(f"Error fetching data: {response.status_code}")
            return None

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data: {e}")
        return None


def transform_data(data: dict, spark):
    """ Transform the schema of the data """
    try:

        all_networks = []

        for network in data["networks"]:
            if network["location"]["country"] in ["US", "GB", "CA"]:

                network_location = network.get("location", {})

                streaming_data =  {
                "network_id": network.get("id"),
                "network_name": network.get("name"),
                "latitude": network_location.get("latitude"),
                "longitude": network_location.get("longitude"),
                "city": network_location.get("city"),
                "country": network_location.get("country"),
                "company": network["company"][0] if "company" in network and network["company"] else None 
                }

                all_networks.append(streaming_data)
                
        schema = StructType([
                StructField("network_id", StringType(), True),
                StructField("network_name", StringType(), True),
                StructField("latitude", FloatType(), True),
                StructField("longitude", FloatType(), True),
                StructField("city", StringType(), True),
                StructField("country", StringType(), True),
                StructField("company", StringType(), True),
                ])


        df = spark.createDataFrame(all_networks, schema=schema)

        logging.info("Data transformed successfully.")

        return df

    except Exception as e:
        logging.error(f"Error transforming schema: {e}")
        return None




   
def network_main():
    spark = None
    try:
        app_name = "fetchAndConsumeNetworksApi"
        url= "http://api.citybik.es/v2/networks"
        spark = intialise_spark_session(app_name)
        data = extract_bike_networks(url)
        table_name = "dim_bike_networks"
        mode = "overwrite"


        df = transform_data(data, spark)

        all_network_ids_df = df.select("network_id").distinct()
        all_network_ids_df = all_network_ids_df.toPandas()

        all_network_ids_df.to_csv("/Workspace/Users/khadijabadmus@yahoo.com/leveraging_azure_for_real_time_city_bike_data_streaming_and_analytics/reference_data/all_networks_id.csv", index=False)

        # Write to database
        write_to_database(df, table_name, mode)

    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        if spark:
            spark.stop()
            logging.info("Spark session stopped.")


if __name__ == "__main__":
    network_main()



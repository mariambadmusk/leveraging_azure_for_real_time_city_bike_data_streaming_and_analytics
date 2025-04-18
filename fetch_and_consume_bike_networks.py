from utils import config_logging, intialise_spark_session,  write_to_database
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import requests
import os


logger = config_logging("extract_and_stream_networks")

coordinates = {
        "lat_min": -90, "lat_max": 90,
        "lon_min": -180, "lon_max": 180
    }

def extract_bike_networks(url) -> None:

    try:
        
        response = requests.get(url)

        if response.status_code == 200:
            logger.info("Data fetched successfully.")
            data = response.json()
            
            return data
        else:
            logger.error(f"Error fetching data: {response.status_code}")
            return None

    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data: {e}")
        return None


def transform_data(data: dict) -> DataFrame:
    """ Transform the schema of the data """
    try:
        all_network_ids = []
        all_networks = []

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

                all_network_ids.append(network.get("id"))
                all_networks.append(streaming_data)
    

        logger.info("Data transformed successfully.")

        return all_networks, all_network_ids

    except Exception as e:
        logger.error(f"Error transforming schema: {e}")
        return None


def create_dataframe(data):
    """"""
    try:
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("latitude", FloatType(), True),
            StructField("longitude", FloatType(), True),
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("company", StringType(), True),
            ])
        
        df = spark.createDataFrame(data, schema=schema)
        logger.info("df created sucessfully")
        return df
    except Exception as e:
        logger.error(f"Error transforming schema: {e}")
        return None
          


def clean_network_data(df: DataFrame) -> DataFrame:
    """Clean bike network data"""
    
    df = df.na.drop(subset=["id"])
    
    # recall
    lat_min, lat_max = coordinates["lat_min"], coordinates["lat_max"]
    lon_min, lon_max = coordinates["lon_min"], coordinates["lon_max"]

    return df.filter(col("latitude").between(lat_min, lat_max) & col("longitude").between(lon_min, lon_max)) \
             .dropDuplicates(["id"])

    
   
def network_main():
    try:
        app_name = "fetchAndConsumeNetworksApi"
        url= "http://api.citybik.es/v2/networks"
        jbdc_url = os.getenv("JDBC_URL")
        table = "bike_networks"

        spark = intialise_spark_session(app_name)
        data = extract_bike_networks(url)

        all_networks, all_network_ids = transform_data(data)
        df = create_dataframe(all_networks)
        cleaned_df = clean_network_df(df)
        
        spark.write.csv(all_network_ids, "reference_data/bike_networks.csv", header=True)

        # Write to database
        write_to_database(cleaned_df, topic, "overwrite", jbdc_url)

    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    network_main()



from utils import config_logging, intialise_spark_session, write_to_database, read_eventhub_stream
from extract_weather import fetch_all_weather
from transform_weather import get_weather_schema, flatten_df, clean_data
import asyncio
import pandas as pd

def weather_main():
    """ Run the main function for the Weather ETL pipeline"""
    spark = None
    try:
        # Set up constants
        logger = config_logging()
        logger.info("Setting up constants for Weather ETL...")

        app_name = "fetchAndConsumeWeatherApi" 
        mode = "append"      
        weather_schema = get_weather_schema()
        street_file_path = ""
        jdbc_url = ""
        properties = ""
        api_key = "350f4d977ad24531a8565131251704"
        if not api_key:
            logger.error("Missing API key for weather data: check your configuration.")
            return
        
        # Set up Event Hub connection string
        weather_event_hub_connstr = "Endpoint=sb://etl-city-weather-api.servicebus.windows.net/;SharedAccessKeyName=weatherPolicy;SharedAccessKey=f5ElB6cChzXHYAnbPsh3vCJ1Rz6xE42dI+AEhBaRMys=;EntityPath=weather_event_hub"
        weather_event_hub_name = "weather_event_hub"

        # Intialise spark session
        spark = intialise_spark_session(app_name)

        if not spark or not weather_event_hub_name or not weather_event_hub_connstr:
            logger.error("Missing spark or event hubs variable configuration: check your configuration.")
            return
        
        # read csv for weather query parameter
        street_df = pd.read_csv(street_file_path )
        street_list  = street_df.squeeze().tolist()

        
        # Read network IDs and fetch data from API
        logger.info("Starting to fetch data from Weather API...")
        asyncio.run(fetch_all_weather(api_key, street_list, weather_event_hub_connstr, weather_event_hub_name))

        # Read from Kafka topic
        logger.info("Reading from Weather hub...")
        streamed_df = read_eventhub_stream(spark, weather_event_hub_connstr, weather_schema)
        
    
        # Transform data
        logger.info("Flatten from Weather hub...")
        flattened_df = flatten_df(streamed_df)
        logger.info("Cleaning Weather hub...")
        cleaned_df = clean_data(flattened_df)

        # Write to database
        logger.info("Writing to database...")
        write_to_database(jdbc_url, properties, cleaned_df, "fact_weather", mode)
        logger.info("Data written to weather table successfully.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        spark.stop() 
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    weather_main()




                
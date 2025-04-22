import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import config_logging, intialise_spark_session, write_to_database, read_eventhub_stream
from extract_stations import fetch_all_stations
from transform_stations import get_station_schema, flatten_df, clean_station_data, create_star_schema_tables
import asyncio
import pandas as pd



async def station_main():
    """ Main function to run the ETL pipeline for bike stations """

    spark = None
    logger = config_logging()

    try:

        logger.info("Setting up constants for Stations etl...")

        app_name = "fetchAndConsumeStationsApi"    
        station_schema = get_station_schema()
        station_event_hub_connstr = "Endpoint=sb://etl-city-weather-api.servicebus.windows.net/;SharedAccessKeyName=stations_api;SharedAccessKey=2sQxBIFrD9nEEzDt+RD3qfCP84tsFLW/C+AEhELJpJ8=;EntityPath=stations_event_hub"
        station_event_hub_name = "stations_event_hub"
        mode = "append"
        network_ids_path = "/dbfs/dbfs/Workspace/Users/khadijabadmus@yahoo.com/city_weather_api/reference_data/bike_networks.csv/all_network_ids.csv"
        network_df = pd.read_csv(network_ids_path)
        network_ids_list  = network_df.squeeze().tolist()

        jdbc_url = ""
        properties = ""

        # Intialise spark session
        spark = intialise_spark_session(app_name)
        
 
        if not spark or station_event_hub_name or not station_event_hub_connstr:
            logger.error("Missing spark or event hubs variable configuration: check your configuration.")
            return
        
        
        # Read network IDs and fetch data from API
        logger.info("Starting to fetch data from Stations API...")
        await fetch_all_stations(station_event_hub_connstr, station_event_hub_name, network_ids_list)
        
        # Read from Event Hub
        logger.info("Reading from Stations hub...")
        streamed_df = read_eventhub_stream(spark, station_event_hub_connstr, station_schema)

        logger.info("Flatten from Stations hub...")
        flattened_df = flatten_df(streamed_df)

        # Transform data
        logger.info("Transforming df and creating tables...")
        cleaned_df = clean_station_data(flattened_df)
        dim_station, fact_time, fact_station = create_star_schema_tables(cleaned_df)
 

        # Write to database
        logger.info("Writing to dim_station, fact_time, fact_station...")
        write_to_database(jdbc_url, properties, dim_station, "dim_station", mode)
        write_to_database(jdbc_url, properties, fact_time, "fact_time", mode)
        write_to_database(jdbc_url, properties, fact_station, "fact_station", mode)
        
        logger.info("Data written to all fact and dimension tables successfully.")
    except Exception as e:
        logger.critical("Pipeline failed", exc_info=True)

    finally:
        spark.stop()    
        logger.info("Spark session and Producer stopped.")




if __name__ == "__main__":
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        loop.create_task(station_main())
    else:
        asyncio.run(station_main())

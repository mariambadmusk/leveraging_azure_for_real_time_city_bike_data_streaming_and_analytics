from utils import *
from fetch_and_consume_bike_stations import *
from transform_stations import *
import os
import asyncio
from configparser import ConfigParser



def station_main():
    """ Main function to run the ETL pipeline for bike stations """
    try:
        # Set up constants
        logger = config_logging()
        config = ConfigParser()
        config.read("path")
        
        logger.info("Setting up constants for Stations etl...")

        app_name = "fetchAndConsumeStationsApi"

        
        station_schema = get_station_schema()
        station_event_hub_connstr = load_secrets()
        station_event_hub_name = config["eventhub_name"]
        station_event_hub_namespace = config["eventhub_namespace"]

        mode = "append"


        # Intialise spark session
        spark = intialise_spark_session(app_name)
 
        if not spark or station_event_hub_name or station_event_hub_namespace or station_event_hub_connstr:
            logger.error("Missing variable configuration: check your configuration.")
            return
        
        # Read network IDs and fetch data from API
        logger.info("Starting to fetch data from API...")
        all_ids = read_network_ids()
        asyncio.run(fetch_all_stations(station_event_hub_name, station_event_hub_namespace, all_ids))
        
        # Read from Kafka topic
        logger.info("Reading from Kafka topic...")
        streamed_df = read_eventhub_stream(spark, station_event_hub_connstr, station_schema)

        # write clean cities to reference weather data
        logger.info("Checking for existing city list...")
        if os.path.exists("reference_data/city_list.csv") == False:
            logger.info("Creating city list...")
            city_df = streamed_df.select("city").distinct()
            city_df.write.csv(
                "reference_data/city_list.csv", header=True, mode="overwrite"
            )
            logger.info("City list created successfully.")
        else:
            logger.info("City list already exists. Skipping creation.")


        # Transform data
        logger.info("Transforming df and creating tables...")
        cleaned_df = clean_station_data(transformed_df)
        dim_station, fact_time, fact_station = create_star_schema_tables(cleaned_df)


        # Write to database
        logger.info("Writing to database...")
        write_to_postgres(dim_station, "dim_station", mode)
        write_to_postgres(fact_time, "fact_time", mode)
        write_to_postgres(fact_station, "fact_station", mode)
        
        logger.info("Data written to all fact and dimensin tables successfully.")
    except Exception as e:
        logger.critical("Pipeline failed", exc_info=True)
    finally:
        spark.stop()    
        logger.info("Spark session and Producer stopped.")


if __name__ == "__main__":
    station_main()

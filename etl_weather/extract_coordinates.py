from utils import config_logging, intialise_spark_session, read_from_database



def extract_unique_coordinates():
    """
    Extract distinct coordinates from database using PySpark and save as CSV.
    """
    spark = None
    try:
        logger = config_logging()
        logger.info("Starting to extract distinct coordinates...")
        
        spark = intialise_spark_session("extractCoordinates")
        table = "dim_weather" 
        csv_path = "reference_data/coordinates.csv"

        df = read_from_database(spark, table)

        coordinates_df = df.select("station_id","latitude", "longitude").distinct()

        coordinates_df.write \
            .mode("overwrite")\
            .option("header", True)\
            .option("delimiter", ",")\
            .option(singleFile=True)\ 
            .csv(csv_path)
        
        logger.info("Data written to reference_data/")

        logger.info("Distinct coordinates extracted and saved successfully.")
    except Exception as e:
        logger.error(f"Error extracting distinct coordinates: {e}")
        return None
    finally:
        spark.stop()


if __name__ == "__main__":
    extract_unique_coordinates()

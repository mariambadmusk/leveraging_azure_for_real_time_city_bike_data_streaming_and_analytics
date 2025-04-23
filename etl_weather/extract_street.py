from utils import config_logging, intialise_spark_session, read_from_database



def extract_unique_streets():
    """
    Extract distinct street names from database using PySpark and save as CSV.
    """
    spark = None
    try:
        logger = config_logging()
        logger.info("Starting to extract distinct street names...")
        
        spark = intialise_spark_session("extractStreet")
        table = "dim_bike_stations" 
        csv_path = ""

        df = read_from_database(spark, table)

        distinct_df = df.select("street").distinct()

        distinct_df.coalesce(1).write \
            .mode("overwrite")\
            .option("header", True)\
            .option("delimiter", ",")\
            .option(singleFile=True)\ 
            .csv(csv_path)
        
        logging.info("Data written to reference_data/")

        logger.info("Distinct street names extracted and saved successfully.")
    except Exception as e:
        logger.error(f"Error extracting distinct street names: {e}")
        return None
    finally:
        spark.stop()


if __name__ == "__main__":
    extract_unique_streets()

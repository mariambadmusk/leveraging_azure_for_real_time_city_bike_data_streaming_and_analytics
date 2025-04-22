# Databricks notebook source
# MAGIC %fs ls /dbfs/Workspace/Users/khadijabadmus@yahoo.com/city_weather_api/reference_data/bike_networks.csv
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# Define the file path
file_path = "dbfs:/Workspace/Users/khadijabadmus@yahoo.com/city_weather_api/reference_data/bike_networks.csv/part-00000-tid-279113605880089999-ec583826-bcd6-440f-9696-a6b8ad7d8619-21-1-c000.csv"

# Read the CSV file into a Spark DataFrame
df = spark.read.format("csv").option("header", True).load(file_path)

# Display the DataFrame
display(df)

# COMMAND ----------

import pandas as pd

df = pd.read_csv("/dbfs/Workspace/Users/khadijabadmus@yahoo.com/city_weather_api/reference_data/bike_networks.csv/all_network_ids.csv", encoding='utf-8')

df.head()

# COMMAND ----------

# %python
# # Define the file path
# file_path = "dbfs:/dbfs/Workspace/Users/khadijabadmus@yahoo.com/city_weather_api/reference_data/bike_networks.csv/all_network_ids.csv"

# # Read the CSV file into a Spark DataFrame
# df = spark.read.format("csv").option("header", True).load(file_path)

# # Display the DataFrame
# display(df)
import pandas as pd
path = "/dbfs/dbfs/Workspace/Users/khadijabadmus@yahoo.com/city_weather_api/reference_data/bike_networks.csv/all_network_ids.csv"

df = pd.read_csv(path)
all_ids = df["network_id"].tolist()


all_ids
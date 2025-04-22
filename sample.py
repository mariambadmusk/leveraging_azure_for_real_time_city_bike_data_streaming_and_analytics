from azure.eventhub import EventHubProducerClient, EventData
import sys
from pyspark.sql import SparkSession


station_event_hub_connstr = "Endpoint=sb://city-bike-etl-pipeline.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=UKmhgZXmqqXyKc/GRqj4wwsjaYVvWxLS8+AEhL6wuhI="
station_event_hub_name = "stations_event_hub"

spark = SparkSession.builder\
            .appName("testStreaming")\
            .getOrCreate()

producer = EventHubProducerClient.from_connection_string(conn_str=station_event_hub_connstr, eventhub_name=station_event_hub_name)
event_data_batch = producer.create_batch()
event_data_batch.add(EventData("Hello World!"))
producer.send_batch(event_data_batch)


sc = spark.sparkContext

eh_conf = {
'eventhubs.connectionString': sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
station_event_hub_connstr),
'eventhubs.startingPosition': '@latest',
'eventhubs.consumerGroup': '$Default',
        }
    
# read from Event Hub
raw_df = spark.readStream \
    .format("eventhubs") \
    .options(**eh_conf) \
    .load()


decoded_df = raw_df.selectExpr("CAST(body AS STRING) as message")


decoded_df.printSchema()


decoded_df.writeStream\
    .outputMode("append")\
    .format("console") \
    .start()\
    .awaitTermination()



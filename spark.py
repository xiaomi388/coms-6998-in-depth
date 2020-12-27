from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import from_json, col, lit
import sys
import uuid
import os
import boto3
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'

#    Spark
from pyspark import SparkContext
#    Spark Streaming
from pyspark.streaming import StreamingContext
#    Kafka
from pyspark.streaming.kafka import KafkaUtils
#    json parsing
import json
from pyspark.sql.types import StructType,StructField,StringType,IntegerType

BOOTSTRAP_SERVERS = "XXXX"
ZK = "XXXX"
ACCESS_ID = "XXXX"
ACCESS_SECRET = "XXXX"

def process_row(row):
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1", aws_access_key_id=ACCESS_ID, aws_secret_access_key=ACCESS_SECRET)
    table = dynamodb.Table("transaction")
    table.put_item(Item=json.loads(row["data"]))


if __name__ == "__main__":
    sc = SparkContext(appName="demo-word-count")
    sc.setLogLevel("WARN")

    schema = StructType([
       StructField("id",StringType(),True),
       StructField("amount",IntegerType(),True),
       StructField("location",StringType(),True),
       StructField("state",StringType(),True),
       StructField("time",IntegerType(),True),
       StructField("user_id",StringType(),True),
       StructField("item",StringType(),True),
       StructField("merchant_number",StringType(),True),
    ])

    spark = SparkSession(sc)
    df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS).option("subscribe", "transaction").option("startingOffsets", "latest").load().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").withColumnRenamed("value", "data").withColumnRenamed("key", "partitionKey")
    df.printSchema()
    query = df.writeStream.foreach(process_row).start()
    query.awaitTermination()

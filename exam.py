#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
 Counts words in UTF8 encoded, '\n' delimited text directly received from Kafka in every 2 seconds.
 Usage: direct_kafka_wordcount.py <broker_list> <topic>

 To run this on your local machine, you need to setup Kafka and create a producer first, see
 http://kafka.apache.org/documentation.html#quickstart

 and then run the example
    `$ bin/spark-submit --jars \
      external/kafka-assembly/target/scala-*/spark-streaming-kafka-assembly-*.jar \
      examples/src/main/python/streaming/direct_kafka_wordcount.py \
      localhost:9092 test`
"""
from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    sc.install_pypi_package("boto3")
    ssc = StreamingContext(sc, 2)

    topic = "transaction"
    brokers = "b-1.in-depth-task3.35ph0c.c9.kafka.us-east-1.amazonaws.com:9092,b-2.in-depth-task3.35ph0c.c9.kafka.us-east-1.amazonaws.com:9092"
    # brokers = "1234.com"
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    lines = kvs.map(lambda x: x[1])
    lines.collect().saveAsTextFile("s3://aws-emr-resources-736025376070-us-east-1/output/")
    # counts = lines.flatMap(lambda line: line.split(" ")) \
    #     .map(lambda word: (word, 1)) \
    #     .reduceByKey(lambda a, b: a+b)
    # counts.pprint()
    # print("over.")

    ssc.start()
    ssc.awaitTermination()


#aws emr add-steps --cluster-id "j-2YCULOHVUH6RY" --steps "Type=spark,Name=step1,Args=[--deploy-mode,cluster,--master,yarn,--conf,spark.yarn.submit.waitAppCompletion=false,--num-executors,2,--executor-cores,2,--executor-memory,1g, s3://aws-emr-resources-736025376070-us-east-1/exam.py],ActionOnFailure=CONTINUE"
#10469  aws emr add-steps --cluster-id "j-1YI6RJ6X75I3P" --steps "Type=spark,Name=step1,Args=[--deploy-mode,cluster,--master,yarn,--conf,spark.yarn.submit.waitAppCompletion=false,--num-executors,2,--executor-cores,2,--executor-memory,1g,s3://spark-bucket-in-depth-task2/test.py],ActionOnFailure=CONTINUE"


#aws emr add-steps --cluster-id "j-2YCULOHVUH6RY" --steps "Type=spark,Name=step1,Args=[--deploy-mode,cluster,--master,yarn,--packages,org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,--conf,spark.yarn.submit.waitAppCompletion=false,--num-executors,2,--executor-cores,2,--executor-memory,1g,s3://aws-emr-resources-736025376070-us-east-1/exam.py],ActionOnFailure=CONTINUE"
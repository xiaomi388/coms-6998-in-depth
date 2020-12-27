from pyspark import SparkContext
import sys

if __name__ == "__main__":
    sc = SparkContext(appName="demo-word-count")
    text_file = sc.textFile("s3://aws-emr-resources-736025376070-us-east-1/exam5.py")
    counts = text_file.flatMap(lambda line: line.split(" ")).map(
        lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    # counts.pprint()
    counts.saveAsTextFile("s3://aws-emr-resources-736025376070-us-east-1/output")
    sc.stop()
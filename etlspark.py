from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf

BUCKET="dataminded-academy-capstone-resources/raw/open_aq/"


config = {"spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.1.2","fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"}

conf = SparkConf().setAll(config.items())


spark = SparkSession.builder.config(conf=conf).getOrCreate()


df= spark.read.json(f"s3a://{BUCKET}/")

df.show()

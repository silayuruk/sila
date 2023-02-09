from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql.functions import flatten

BUCKET="dataminded-academy-capstone-resources/raw/open_aq/"


config = {"spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.1.2,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3,net.snowflake:snowflake-jdbc:3.13.22","fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"}

conf = SparkConf().setAll(config.items())


spark = SparkSession.builder.config(conf=conf).getOrCreate()


df= spark.read.json(f"s3a://{BUCKET}/")

df.show()

df2=df

flatten_df = df.select("*", "date.local", "date.utc")

flatten_df.show() 

print(flatten_df.printSchema())

# Flatten
final_df = flatten_df.select("*", 'coordinates.*')
final_df = final_df.drop('coordinates','date')

print(final_df.printSchema())

import botocore 
import botocore.session 
from aws_secretsmanager_caching import SecretCache, SecretCacheConfig 

client = botocore.session.get_session().create_client('secretsmanager')
cache_config = SecretCacheConfig()
cache = SecretCache( config = cache_config, client = client)

secret = cache.get_secret_string('snowflake/capstone/login')

print('secret'+ secret)

import json

creds = json.loads(secret)

snowflake_pkgs =["net.snowflake:snowflake-jdbc:3.13.22",
                 "net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3"]


SCHEMA = "SILA"
SNOWFLAKE_SOURCE_NAME="net.snowflake.spark.snowflake"


sfOptions = {
"sfURL": f"{creds['URL']}",
"sfPassword": creds["PASSWORD"],
"sfUser": creds["USER_NAME"],
"sfDatabase": creds["DATABASE"],
"sfWarehouse": creds["WAREHOUSE"],
"sfRole": creds["ROLE"],
'sfSchema': SCHEMA
}




(

    final_df
        .write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(**sfOptions)
        .option('dbtable', "weatherr")
        .mode("overwrite")
        .save()

)
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
spark = SparkSession.builder\
        .master("local")\
        .appName("Colab")\
        .config('spark.ui.port', '4050')\
        .getOrCreate()
spark

import pyspark.sql.functions as F

import os
import logging
import boto3
from botocore.exceptions import ClientError
from KEYS import ACCESS_KEY, SECRET_ACCESS_KEY

s3_client = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_ACCESS_KEY
    )

def upload_file(file_name, bucket, object_name=None):
    if object_name is None:
        object_name = os.path.basename(file_name)

    try:
        res = s3_client.upload_file(file_name, bucket, object_name)
        if res:
            print("Successfully uploaded file")
    except ClientError as e:
        logging.error(e)
        return False
    return True

def download_file(file_name, bucket, object_name):
    try:
        res = s3_client.download_file(bucket, object_name, file_name)
        if res:
            print("Successfully downloaded file")
    except ClientError as e:
        logging.error(e)
        return False
    return True

bucket = "capstone-project-team2"
file1 = "LOL_items_stats.csv"
file2 = "lol_champion_stats_en.csv"
download_file(file1, bucket=bucket, object_name=file1)
download_file(file2, bucket=bucket, object_name=file2)

df1 = spark.read.csv(file1,encoding='ISO-8859-1', header=True, sep =';')
df2 = spark.read.csv(file2,encoding='ISO-8859-1', header=True, sep =';')

#changing the data types to float
from pyspark.sql.functions import *
cols = ["Cost","Sell", 'AS', 'Crit', 'LS', 'APen', 'AP','AH', 'Mana','MP5','HSP','OVamp', 'MPen', 'Health', 'Armor',"MR","HP5","MS"]
for col_name in cols:
    df1 = df1.withColumn(col_name, col(col_name).cast('float'))

#Making Item column primary key(nullable=false)
df1 = df1.withColumn("Item", F.coalesce(F.col("Item"), F.lit(0)))

#replacing null value to unknown for Maps Column
df1 = df1.na.fill(value="Unknown", subset=["Maps"])

# Displaying table
df1.printSchema()
df1.show(200)


# Converting champion_name to non-nullable
df2 = df2.withColumn("champion_name", F.coalesce(F.col("champion_name"), F.lit(0)))

#Changing date release to date datatype
df2 = df2.withColumn('date_release', F.to_timestamp('date_release', 'dd/MM/yyyy'))
df2 = df2.withColumn('date_release', F.to_date(col("date_release"),"dd-MM-yyyy"))

# Converting columns to double
cols = cols = [ 'HP', 'HP+', 'HP_lvl18', 'HP5', 'HP5+', 'HP5_lvl18', 'MP', 'MP+', 'MP_lvl18', 'MP5', 'MP5+', 'MP5_lvl18', 'AD', 'AD+', 'AD_lvl18', 'AS', 'AS_lvl18', 'AR', 'AR+', 'AR_lvl18', 'MR', 'MR+', 'MR_lvl18', 'MS', 'MS_lvl18', 'range', 'range_lvl18']
for col_name in cols:
    df2 = df2.withColumn(col_name, col(col_name).cast('float'))

# Displaying table
df2.printSchema()
df2.show()
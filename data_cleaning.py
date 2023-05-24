from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()

# Configure SparkSession
spark = SparkSession.builder\
        .master("local")\
        .appName("Colab")\
        .config('spark.ui.port', '4050')\
        .getOrCreate()

# Print SparkSession
spark

import pyspark.sql.functions as F
import os
import logging
import boto3
from botocore.exceptions import ClientError
from KEYS import ACCESS_KEY, SECRET_ACCESS_KEY

# Create S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_ACCESS_KEY
)

def upload_file(file_name, bucket, object_name=None):
    """
    Function to upload a file to S3 bucket.
    Args:
        file_name (str): Local file path.
        bucket (str): S3 bucket name.
        object_name (str, optional): Object name in S3. If not provided, the file name will be used.
    Returns:
        bool: True if the file upload is successful, False otherwise.
    """
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
    """
    Function to download a file from S3 bucket.
    Args:
        file_name (str): Local file path.
        bucket (str): S3 bucket name.
        object_name (str): Object name in S3.
    Returns:
        bool: True if the file download is successful, False otherwise.
    """
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

# Download files from S3 bucket
download_file(file1, bucket=bucket, object_name=file1)
download_file(file2, bucket=bucket, object_name=file2)

# Read CSV files into DataFrames
df1 = spark.read.csv(file1, encoding='ISO-8859-1', header=True, sep=';')
df2 = spark.read.csv(file2, encoding='ISO-8859-1', header=True, sep=';')

# Changing the data types to float
from pyspark.sql.functions import *
cols = ["Cost","Sell", 'AS', 'Crit', 'LS', 'APen', 'AP','AH', 'Mana','MP5','HSP','OVamp', 'MPen', 'Health', 'Armor',"MR","HP5","MS"]
for col_name in cols:
    df1 = df1.withColumn(col_name, col(col_name).cast('float'))

# Making Item column primary key (nullable=false)
df1 = df1.withColumn("Item", F.coalesce(F.col("Item"), F.lit(0)))

# Replacing null values with "Unknown" for Maps column
df1 = df1.na.fill(value="Unknown", subset=["Maps"])

# Displaying table schema and data
df1.printSchema()
df1.show(200)


# Converting champion_name to non-nullable
df2 = df2.withColumn("champion_name", F)
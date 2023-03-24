import pandas as pd
import os
import logging
import boto3
from botocore.exceptions import ClientError
from KEYS import ACCESS_KEY, SECRET_ACCESS_KEY


import tempfile

# create a temporary file and write some data to it


#reading the key from AWS account
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

#calling the function/downloading the file
bucket = "capstone-project-team2"
file1 = "LOL_items_stats.csv"
file2 = "lol_champion_stats_en.csv"
download_file(file1, bucket=bucket, object_name=file1)
download_file(file2, bucket=bucket, object_name=file2)

#reading the file using pandas

df1 = pd.read_csv(file1,encoding='ISO-8859-1', delimiter=';')
df2 = pd.read_csv(file2,encoding='ISO-8859-1', delimiter=';')

#data cleaning / Stat Dataset
# Converting champion_name to non-nullable
#changing the data types to float
cols = ["Cost","Sell", 'AS', 'Crit', 'LS', 'APen', 'AP','AH', 'Mana','MP5','HSP','OVamp', 'MPen', 'Health', 'Armor',"MR","HP5","MS"]
for col_name in cols:
    df1[col_name] = df1[col_name].astype("float")

#Making Item column primary key(nullable=false)
#df1 = df1.withColumn("Item", F.coalesce(F.col("Item"), F.lit(0)))

#replacing null value to unknown for Maps Column
#df1 = df1.na.fill(value="Unknown", subset=["Maps"])
df1["Maps"].fillna("Unknown", inplace = True)

# Displaying table
print(df1.info())
print(df1.head())

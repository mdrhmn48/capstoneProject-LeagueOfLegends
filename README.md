This README provides instructions for setting up PySpark and performing various operations using PySpark. Follow the steps below to get started:

Install PySpark:
        Run the command pip install -q pyspark to install PySpark.

Set up SparkSession:
        Import the required libraries: from pyspark.sql import SparkSession
        Create a SparkSession: spark = SparkSession.builder.master("local[*]").getOrCreate()
            This sets up a SparkSession in local mode with all available cores.
            Alternatively, you can customize the configuration based on your needs.

Import necessary libraries:
        To perform operations on PySpark DataFrames, you can import the pyspark.sql.functions module as import pyspark.sql.functions as F.

Read from and write to Amazon S3 bucket (optional):
        If you need to read from or write to an Amazon S3 bucket, you can use the boto3 library.
        Install boto3 using the command !pip install boto3.
        Import the required libraries: import os, import logging, import boto3.
        Add your access key and secret access key to the KEYS.py file or provide them directly in the code.
        Define the s3_client using the access key and secret access key.
        Utilize the upload_file() and download_file() functions to upload/download files from the S3 bucket.

Data Cleaning:
        The code provides an example of cleaning the 'Stats' dataset.
        It performs operations such as changing data types, handling null values, and displaying the cleaned DataFrame.

Reading from CSV:
        Use the spark.read.csv() function to read the CSV files into PySpark DataFrames.
        Adjust the encoding, header, and separator as needed.

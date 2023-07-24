# Tableau Visualization: 

![Tableau Dashboard](https://github.com/mdrhmn48/capstoneProject-LeagueOfLegends/blob/master/Screenshot_20230310_095823.png)

Interactive Dashboard link: 
        
        https://public.tableau.com/app/profile/md.rahman5916/viz/captone_project2/Dashboard22

# Spark Data Processing with S3 Integration

This repository contains a Python script for data processing with Apache Spark and integration with Amazon S3. The script reads CSV files from an S3 bucket, performs data transformations, and showcases Spark's capabilities for data analysis. The primary purpose of this script is to process data related to League of Legends (LoL) items and champion statistics.
Prerequisites

Before running the script, ensure you have the following components installed:
        1. Python (version 3.6 or higher)
        2. Apache Spark (version 2.4 or higher)
        3. pyspark Python package (for Spark integration)
        4. boto3 Python package (for Amazon S3 integration)

# Setup
  1. Clone this repository to your local machine or server.
  2. Make sure you have access to the S3 bucket where the CSV files are located.
  3. Obtain the ACCESS_KEY and SECRET_ACCESS_KEY required for connecting to the S3 bucket. (Note: Keep these credentials secure and do not share them.)

# Usage
 1. Open the Python script and make sure to provide the correct S3 bucket name (bucket) and file names (file1, file2) as required.
 2. Install the necessary Python packages if you haven't already: 

        pip install pyspark boto3.

        Run the script: python data_cleaning.py
# The script will perform the following tasks:
    1. Create a SparkSession to initiate the Spark context.
    2. Connect to the specified S3 bucket using the provided credentials.
    3. Download two CSV files (LOL_items_stats.csv and lol_champion_stats_en.csv) from the S3 bucket to the local machine or server.
    4. Read the CSV files into Spark DataFrames (df1 and df2).
    5. Perform data transformations on df1 to change specific columns' data types to float, make the "Item" column primary key (nullable=false), and replace null values in the "Maps" column with "Unknown".
    6. Display the schema and data of df1.

Note: The script contains some incomplete code for processing df2. Please update it according to your specific data processing needs for the "champion_name" column.
# Contributors:

    Your Name: MD Rahman

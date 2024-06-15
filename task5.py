import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import from_unixtime, date_format
from pyspark.sql.functions import to_date, count, col, year, month, sum, row_number, avg, dayofmonth, regexp_replace
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
from graphframes import *
from pyspark.sql.functions import concat_ws, col

#SPARK Configuration
if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("NYC")\
        .getOrCreate()
    
    def good_ride_line(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            #int(fields[0])
            return True
        except:
            return False
            
    def good_taxi_line(line):
        try:
            fields = line.split(',')
            if len(fields)!=4:
                return False
            #int(fields[0])
            return True
        except:
            return False
  
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    
# Load rideshare_data.csv 
    ride_lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/rideshare_data.csv")
    clean_ride_lines = ride_lines.filter(good_ride_line)
    ride_features = clean_ride_lines.map(lambda l: tuple(l.split(',')))
    header_ride = ride_features.first()
    ride_data_RDD = ride_features.filter(lambda row: row!=header_ride)
    column_names = ["business", "pickup_location", "dropoff_location", "trip_length", "request_to_pickup", "total_ride_time", 
        "on_scene_to_pickup", "on_scene_to_dropoff", "time_of_day", "date", "passenger_fare", "driver_total_pay", "rideshare_profit", 
         "hourly_rate", "dollars_per_mile"]
    ride_df = ride_data_RDD.toDF(column_names)

# Load taxi_zone_lookup.csv.
    taxi_lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/taxi_zone_lookup.csv")
    clean_taxi_lines = taxi_lines.filter(good_taxi_line)
    taxi_features = clean_taxi_lines.map(lambda l: tuple(l.split(',')))
    header_taxi = taxi_features.first()
    taxi_data_RDD = taxi_features.filter(lambda row: row!=header_taxi)
    column_names_taxi = ["LocationID", "Borough", "Zone", "service_zone"]
    taxi_df = taxi_data_RDD.toDF(column_names_taxi)

    # Removing double quotes from the taxi zone look up table values
    for column in column_names_taxi:
        taxi_df = taxi_df.withColumn(column, regexp_replace(col(column), '\"', ''))

# Joining the datasets
    # Performing join on pickup_location and LocationID
    joined_df1 = ride_df.join(taxi_df, ride_df.pickup_location == taxi_df.LocationID)
    joined_df_final = joined_df1.withColumnRenamed("Borough", "Pickup_Borough")\
                                 .withColumnRenamed("Zone", "Pickup_Zone")\
                                 .withColumnRenamed("service_zone", "Pickup_service_zone")\
                                 .drop("LocationID")

    # Performing join on dropoff_location and LocationID
    joined_df2 = joined_df_final.join(taxi_df, joined_df_final.dropoff_location == taxi_df.LocationID)
    nyc_df = joined_df2.withColumnRenamed("Borough", "Dropoff_Borough")\
                                 .withColumnRenamed("Zone", "Dropoff_Zone")\
                                 .withColumnRenamed("service_zone", "Dropoff_service_zone")\
                                 .drop("LocationID")
    
    # To convert the UNIX timestamp to the "yyyy-MM-dd" format
    nyc_df= nyc_df.withColumn("date", date_format(from_unixtime("date"), "yyyy-MM-dd"))

    # Converting 'date' column to date type
    nyc_df = nyc_df.withColumn("date", to_date(nyc_df.date, "yyyy-MM-dd"))
    
    # Extracting month from the date    
    nyc_df1 = nyc_df.withColumn("month", month("date"))

# TASK 5: Finding anomalies

#1} Extract the data in January and calculate the average waiting time

    # Filtering data for January
    jan_df = nyc_df1.filter(month("date") == 1)

    # Calculating the average waiting time per day
    avg_waiting_time_per_day = jan_df.groupBy(dayofmonth("date").alias("day")) \
                                     .agg(avg("request_to_pickup").alias("average_waiting_time")) \
                                     .orderBy("day")
    # Result
    avg_waiting_time_per_day.show()

    # Created resource object for S3 bucket for storing average waiting time per day data in S3 bucket
    bucket = boto3.resource(
        "s3",
        endpoint_url="http://" + s3_endpoint_url,
        aws_access_key_id=s3_access_key_id,
        aws_secret_access_key=s3_secret_access_key,
    )

    # To specify date and time in the file name
    now = datetime.now()
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    # To combine all the partition documents as single file
    output_df = avg_waiting_time_per_day.coalesce(1)
    
    # creating the S3 path for storing the result
    output_path = "s3a://" + s3_bucket + "/bigdata_" + date_time + "/waiting_time_per_day.csv"
    
    # Save the DataFrame to CSV on S3
    output_df.write.csv(path=output_path, mode="overwrite", header=True)

    # After the program execution, execute this below command to copy file from S3 bucket locally in the hub
    # ccc method bucket ls - to check our file exists or not and copy the file name
    # ccc method bucket cp -r bkt:your_directory_name output_directory_name 

    # Once the output directory stored in our system, we can able to plot histogram using matplotlib 


#2} The day which has the average waiting time exceed 300 seconds

    day_greater_300_secs = avg_waiting_time_per_day.filter(col("average_waiting_time") > 300)

    # Result
    day_greater_300_secs.show()

    spark.stop()







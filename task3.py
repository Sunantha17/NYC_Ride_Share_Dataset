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
from pyspark.sql.functions import to_date, count, col, year, month, sum, row_number, regexp_replace
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
    
    #convert the UNIX timestamp to the "yyyy-MM-dd" format
    nyc_df= nyc_df.withColumn("date", date_format(from_unixtime("date"), "yyyy-MM-dd"))

    
    # Converting 'date' column to date type
    nyc_df = nyc_df.withColumn("date", to_date(nyc_df.date, "yyyy-MM-dd"))
    

    # Extract month from the date    
    nyc_df1 = nyc_df.withColumn("month", month("date"))


# TASK 3 - TOP K processing

#1}Top 5 popular pickup boroughs each month

    #Group and aggregate
    borough_monthly_pickup_trips = nyc_df1.groupBy("Pickup_Borough", "month").agg(count("*").alias("trip_count"))
    
    #Defining a window spec
    windowSpec = Window.partitionBy("month").orderBy(borough_monthly_pickup_trips["trip_count"].desc())

    #Applying row_number over the window spec and filter for top 5
    top5_borough_monthly_pickup_trips = borough_monthly_pickup_trips.withColumn("row_num", row_number().over(windowSpec))\
                                                       .filter("row_num <= 5").drop("row_num")

    #top 5 pickup boroughs in each month by trip count, sorted first by month and then by trip count in descending order.
    top5_borough_monthly_pickup_trips = top5_borough_monthly_pickup_trips.orderBy("month", "trip_count", ascending=[True, False])

    #Result
    top5_borough_monthly_pickup_trips.show(60)


#2}Top 5 popular dropoff boroughs each month

    #Group and aggregate
    borough_monthly_dropoff_trips = nyc_df1.groupBy("Dropoff_Borough", "month").agg(count("*").alias("trip_count"))
    
    # Define a window spec
    windowSpec = Window.partitionBy("month").orderBy(borough_monthly_dropoff_trips["trip_count"].desc())

    # Applying row_number over the window spec and filter for top 5
    top5_borough_monthly_dropoff_trips = borough_monthly_dropoff_trips.withColumn("row_num", row_number().over(windowSpec))\
                                                    .filter("row_num <= 5").drop("row_num")


    #top 5 pickup boroughs in each month by trip count, sorted first by month and then by trip count in descending order.
    top5_borough_monthly_dropoff_trips = top5_borough_monthly_dropoff_trips.orderBy("month", "trip_count", ascending=[True, False])

    #Result
    top5_borough_monthly_dropoff_trips.show(60)

#3}Top 30 earnest routes

    # Adding new column 'Route' that concatenates 'Pickup_Borough' and 'Dropoff_Borough'
    nyc_df1_earnest = nyc_df1.withColumn("Route", concat_ws(" to ", col("Pickup_Borough"), col("Dropoff_Borough")))\
                             .withColumn("driver_total_pay", col("driver_total_pay").cast("float"))

    # Grouping by 'Route' and computing the sum of 'driver_total_pay'
    route_profits = nyc_df1_earnest.groupBy("Route").agg(sum("driver_total_pay").alias("total_profit"))

    # Ordering by 'total_profit' in descending order and taking the top 30
    top30_routes = route_profits.orderBy(col("total_profit").desc()).limit(30)

    #final 30 earnest routes result
    top30_routes.show(30, truncate=False)

    spark.stop()






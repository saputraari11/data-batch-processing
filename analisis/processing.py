from pyspark.sql import SparkSession,functions as F
from pyspark.sql.types import *
import argparse

parser = argparse.ArgumentParser()

parser.add_argument("--input_green",required=True)
parser.add_argument("--input_yellow",required=True)
parser.add_argument("--output",required=True)

args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output

spark = SparkSession.builder.appName("processing_data").getOrCreate()

schema_green = StructType([
    StructField('VendorID', IntegerType(), True), 
    StructField('lpep_pickup_datetime', TimestampType(), True), 
    StructField('lpep_dropoff_datetime', TimestampType(), True), 
    StructField('store_and_fwd_flag', StringType(), True), 
    StructField('RatecodeID', IntegerType(), True), 
    StructField('PULocationID', IntegerType(), True), 
    StructField('DOLocationID', IntegerType(), True), 
    StructField('passenger_count', IntegerType(), True), 
    StructField('trip_distance', DoubleType(), True), 
    StructField('fare_amount', DoubleType(), True), 
    StructField('extra', DoubleType(), True), 
    StructField('mta_tax', DoubleType(), True), 
    StructField('tip_amount', DoubleType(), True), 
    StructField('tolls_amount', DoubleType(), True), 
    StructField('ehail_fee', DoubleType(), True), 
    StructField('improvement_surcharge', DoubleType(), True), 
    StructField('total_amount', DoubleType(), True), 
    StructField('payment_type', IntegerType(), True), 
    StructField('trip_type', IntegerType(), True), 
    StructField('congestion_surcharge', DoubleType(), True)
])

schema_yellow = StructType([
    StructField('VendorID', IntegerType(), True), 
    StructField('tpep_pickup_datetime', TimestampType(), True), 
    StructField('tpep_dropoff_datetime', TimestampType(), True), 
    StructField('passenger_count', IntegerType(), True), 
    StructField('trip_distance', DoubleType(), True), 
    StructField('RatecodeID', IntegerType(), True), 
    StructField('store_and_fwd_flag', StringType(), True), 
    StructField('PULocationID', IntegerType(), True), 
    StructField('DOLocationID', IntegerType(), True), 
    StructField('payment_type', IntegerType(), True), 
    StructField('fare_amount', DoubleType(), True), 
    StructField('extra', DoubleType(), True), 
    StructField('mta_tax', DoubleType(), True), 
    StructField('tip_amount', DoubleType(), True), 
    StructField('tolls_amount', DoubleType(), True), 
    StructField('improvement_surcharge', DoubleType(), True), 
    StructField('total_amount', DoubleType(), True), 
    StructField('congestion_surcharge', DoubleType(), True)
])

df_green = spark.read.option("header",True).schema(schema_green).parquet(input_green)
df_yellow = spark.read.option("header",True).schema(schema_yellow).parquet(input_yellow)


df_green = df_green.withColumnRenamed('lpep_pickup_datetime','pickup_datetime').withColumnRenamed("lpep_dropoff_datetime","dropoff_datetime")
df_yellow = df_yellow.withColumnRenamed('tpep_pickup_datetime','pickup_datetime').withColumnRenamed("tpep_dropoff_datetime","dropoff_datetime")

intersect_column = []

yellow_column = set(df_yellow.columns)

for col in df_green.columns:
    if col in yellow_column:
        intersect_column.append(col)


df_green = df_green.select(intersect_column).withColumn("service_type",F.lit("green"))
df_yellow = df_yellow.select(intersect_column).withColumn("service_type",F.lit("yellow"))
df_trips_data = df_green.union(df_yellow)


df_trips_data.createOrReplaceTempView("trips_data")

df_result = spark.sql("""
SELECT 
    -- Reveneue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(passenger_count) AS avg_montly_passenger_count,
    AVG(trip_distance) AS avg_montly_trip_distance
FROM
    trips_data
GROUP BY
    1, 2, 3
""")


df_result.coalesce(1).write.parquet(output,mode="overwrite")
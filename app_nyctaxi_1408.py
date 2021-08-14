# -*- coding: utf-8 -*-
import pyspark
import os
import sys
import datetime
############################################################################
from pyspark import SparkContext, SQLContext, SparkConf
from pyspark.sql import functions as f
from pyspark.sql.functions import to_timestamp, col, udf
#############################################################################

#########################################################
# Parameters
#########################################################
parameters = {  
    "SparkAppName": "App_NYC_Taxi",
    "TRIPS_JSON" :    "sprints_landing/nyctaxi_trips/json/*/*",
    "VENDOR_CSV" :    "sprints_landing/nyctaxi_trips/csv/vendor",
    "PAYMENT_CSV":    "sprints_landing/nyctaxi_trips/csv/payment",
    "SILVER_TRIPS":   "sprints_silver/nyctaxi_trips/trips",
    "SILVER_VENDOR":  "sprints_silver/nyctaxi_trips/vendor",
    "SILVER_PAYMENT": "sprints_silver/nyctaxi_trips/payment",
    "TEMP_BUCKET":    "dataproc_temp_001"
    }
parameters["BUCKET"] = str(parameters["TEMP_BUCKET"])


#########################################################
# Functions
#########################################################

### Write Spark dataframe to Google Cloud Storage
### You must give the parameters such as df,path in gcs(bucket/directory), boolean partition True or False,
### and field_partition to specified field you wanna to write parquet file.

def writeData(df, path, mode, fileFormat, partition, field_partition=None):
    path = "gs://" + path
    if partition==False:
        df.coalesce(1).write.mode(str(mode)).format(str(fileFormat)).save(str(path))
    else:
        df.write.partitionBy(field_partition).mode(str(mode)).format(str(fileFormat)).save(str(path))
#########################################################

### Write Spark Dataframe to BigQuery
### You must give the follow parameters df, partition field, mode (overwrite or append) 
### and dataset_table (dataset name and table name on BigQuery) 

def writeBigQuery(df,partition,mode, dataset_table):
    df.write.partitionBy(partition) \
    .mode(str(mode)).format('bigquery') \
    .option('table', str(dataset_table)) \
    .save()
#########################################################

### Read data in many files formats in Cloud Storage such as Parquet, Orc, CSV, JSON on the same function
### It's returns a Spark Dataframe
    
def readData(file_type, path, delimiter = ","):
    if (file_type == "orc"):
        df = sqlContext.read.orc("gs://" + path )
    elif (file_type == "parquet"):
        df = sqlContext.read.parquet("gs://" + path )
    elif (file_type == "json"):
        df = sqlContext.read.json("gs://" + path )
    elif (file_type == "csv"):
        df = sqlContext.read.format("csv").option("delimiter", delimiter).option("header", "true").load("gs://" + path )
    return df
#########################################################

### It's returns a Spark SQL Temp View for SQL executions purpose 
### You can give parameters such as viewName, path on Cloud Storage and fileType (CSV, JSON, PARQUET, ORC)
def createView(viewName, path, fileType):        
    df_view = readData(fileType, path)
    df_view.createOrReplaceTempView(viewName)
    return df_view
#########################################################
### You execute SQL Context throuth this function, it's returns a Spark dataframe
def execSparkSQL(query):
    df = sqlContext.sql(query)
    return df
#########################################################
### Function for configuring Spark Context, you can inform any spark conf to Spark application

def OpenSparkContext(appName):
    conf = SparkConf()    
    conf.setAppName(appName)
    conf.set('temporaryGcsBucket', parameters["BUCKET"])
    return conf    
#########################################################

#########################################################
# End of Functions
#########################################################

#########################################################
# Queries
#########################################################

### Queries for executing transformations

payQuery =  ("""select A as payment_type, B as payment_lookup   
                      from tb_payments    where A<>'payment_type' AND B<>'payment_lookup'""")

trips_vendorsQuery=("""select 
                         t.dropoff_datetime, 
                         extract(year from dropoff_datetime) dropoff_datetime_year,
                         extract(month from dropoff_datetime) dropoff_datetime_month,
                         extract(day from dropoff_datetime) dropoff_datetime_day,
                         extract(hour from dropoff_datetime) dropoff_datetime_hour,
                         extract(minute from dropoff_datetime) dropoff_datetime_minute,
                         t.dropoff_latitude, t.dropoff_longitude,
                         t.passenger_count, t.payment_type, 
                         t.pickup_datetime,
                         extract(year from pickup_datetime)   pickup_datetime_year,
                         extract(month from pickup_datetime)  pickup_datetime_month,
                         extract(day from pickup_datetime)    pickup_datetime_day,
                         extract(hour from pickup_datetime)   pickup_datetime_hour,
                         extract(minute from pickup_datetime) pickup_datetime_minute,
                         t.pickup_latitude, t.pickup_longitude,
                         t.rate_code, t.store_and_fwd_flag, t.surcharge,
                         t.tip_amount, t.tolls_amount, t.total_amount,
                         t.trip_distance, t.vendor_id,
                         v.name, v.address, v.city, v.state, 
                         v.zip, v.country, v.contact, v.current
                     from tb_trips_parquet t 
                                       left join tb_vendors_parquet v 
                                              on upper(trim(t.vendor_id))=upper(trim(v.vendor_id))""")

#########################################################

#########################################################
# Start execution
#########################################################

if __name__ == '__main__':
    try:

        # Start Spark Context
        config = OpenSparkContext(parameters["SparkAppName"])
        sc = SparkContext.getOrCreate(conf=config)
        sqlContext = SQLContext(sc)
        sc.setLogLevel("WARN")
        
        # Read data from Cloud Storage
        df_trips    = createView("tb_trips"    , parameters["TRIPS_JSON"] , "json")
        df_vendors  = createView("tb_vendors"  , parameters["VENDOR_CSV"] , "csv")
        df_payments = createView("tb_payments" , parameters["PAYMENT_CSV"], "csv")
        
        # Execute transformations on payment dataset
        df_pay= execSparkSQL(payQuery)
        
        # Once data read from source files, 
        # we have to save these dataframes as parquet format to another bucket in Data lake
        writeData(df_trips,   parameters["SILVER_TRIPS"],  "overwrite", "parquet", True, "vendor_id")
        writeData(df_vendors, parameters["SILVER_VENDOR"], "overwrite", "parquet", False, None)
        writeData(df_pay,     parameters["SILVER_PAYMENT"],"overwrite", "parquet", False, None)
        
        # Now we can read data from parquet format, it's better for new tranformations and performance   
        df_trips_parquet    = createView("tb_trips_parquet",     parameters["SILVER_TRIPS"],   "parquet")
        df_vendors_parquet  = createView("tb_vendors_parquet",   parameters["SILVER_VENDOR"],  "parquet")
        df_payments_parquet = createView("tb_payments_parquet",  parameters["SILVER_PAYMENT"], "parquet")
          
        # In this case, I thought about to do a join between trips and vendor to one dataframe
        df_trips_vendors = execSparkSQL(trips_vendorsQuery)
        
        # Finally I did write the results to BigQuery for data analysis 
        writeBigQuery(df_trips_vendors,["dropoff_datetime_year", "dropoff_datetime_month"],"overwrite", "data_sprints.trips_vendors")

    except Exception as e:
        print (e)
        sc.stop()
    else:
        ### Clean the temp views
        for table in sqlContext.tableNames():
            sqlContext.dropTempTable(str(table))
        ### Stop Spark Context    
        sc.stop()





# Databricks notebook source
# MAGIC %md
# MAGIC ##### Create a  date parameter to ingest daily data

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Open coordinates file to obtain the file names used in the ingestion process

# COMMAND ----------

import json
import pandas as pd

# COMMAND ----------

    with open("/dbfs/mnt/weatherprojectdl/coordinates/geographical_coordinates.json", 'r') as f:
      data = f.read()

    city_information = json.loads(data)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create the ingestion function

# COMMAND ----------

all_dfs =[]

# COMMAND ----------

def ingest_data(ingestion_date, file_name):
        # load the json file
        with open(f"/dbfs/mnt/weatherprojectdl/raw/{ingestion_date}/{file_name}.json", 'r') as f:
            data = f.read()
        # convert the json file to a python object
        api_json = json.loads(data)
        # normalize the nested json file using pandas   
        api_norm = pd.json_normalize(api_json)
        # append the dataframe to a list
        all_dfs.append(api_norm)



# COMMAND ----------

for i in city_information:
    file_nm = i["city_name"]
    ingest_data(v_file_date, file_nm)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Concatenate all dataframes to create a single one

# COMMAND ----------

concat_df = pd.concat(all_dfs)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Use the pandas explode method to create a row per record

# COMMAND ----------

explode_df = concat_df.explode(["hourly.time","hourly.temperature_2m","hourly.relativehumidity_2m","hourly.rain","hourly.snowfall","hourly.windspeed_10m"])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Cast the desirable columns to the correct data types

# COMMAND ----------

explode_df["hourly.time"]=pd.to_datetime(explode_df["hourly.time"])
explode_df["hourly.temperature_2m"]=pd.to_numeric(explode_df["hourly.temperature_2m"])
explode_df["hourly.relativehumidity_2m"]=pd.to_numeric(explode_df["hourly.relativehumidity_2m"])
explode_df["hourly.rain"]=pd.to_numeric(explode_df["hourly.rain"])
explode_df["hourly.snowfall"]=pd.to_numeric(explode_df["hourly.snowfall"])
explode_df["hourly.windspeed_10m"]=pd.to_numeric(explode_df["hourly.windspeed_10m"])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read the dataframe with spark

# COMMAND ----------

data_df = spark.createDataFrame(explode_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Rename columns

# COMMAND ----------

renamed_df = data_df.withColumnRenamed("hourly.time","date") \
                     .withColumnRenamed("hourly.temperature_2m","temperature") \
                     .withColumnRenamed("hourly.relativehumidity_2m","humidity") \
                     .withColumnRenamed("hourly.rain","rain") \
                     .withColumnRenamed("hourly.snowfall","snowfall") \
                     .withColumnRenamed("hourly.windspeed_10m","wind_speed")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Add an ingestion date column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

ingestion_date_df = renamed_df.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Select columns for the final dataframe

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

final_df = ingestion_date_df.select(col("latitude"),
                             col("longitude"),
                             col("date"),
                             col("temperature"),
                             col("humidity"),
                             col("rain"),
                             col("snowfall"),
                             col("wind_speed"),
                             col("ingestion_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create a function to update the *weather_processed.weather_data* table with the new incoming records

# COMMAND ----------

def merge_delta_data(input_df,db_name, table_name,folder_path,merge_condition):    
    from delta.tables import DeltaTable
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
        deltaTable.alias('tgt').merge(
            input_df.alias('src'),
            merge_condition) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()             
        
    else:
        input_df.write.mode("overwrite").format("delta").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

merge_condition = "tgt.latitude = src.latitude AND tgt.longitude = src.longitude AND tgt.date = src.date"
processed_folder_path = "/mnt/weatherprojectdl/processed"
merge_delta_data(final_df,"weather_processed","weather_data",processed_folder_path,merge_condition)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Query to validate that the number of records equals the expected number of records

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATEDIFF(NOW(),'1990-01-01')*264 == COUNT(*)  as expected_records_match, 
# MAGIC MAX(`date`) as data_max_date
# MAGIC FROM weather_processed.weather_data

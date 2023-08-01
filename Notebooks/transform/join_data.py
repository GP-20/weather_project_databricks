# Databricks notebook source
# MAGIC %md
# MAGIC ##### Create a dataframe with the new ingested data

# COMMAND ----------

incoming_data_df = spark.sql("""
                             SELECT c.city_name,
                                    w.`date` AS reported_date,
                                    w.temperature,
                                    w.humidity,
                                    w.rain,
                                    w.snowfall,
                                    w.wind_speed
                             FROM weather_processed.weather_data w
                             JOIN weather_processed.coordinates c
                             ON w.latitude=c.latitude AND w.longitude=c.longitude
                             WHERE ingestion_date = (SELECT max(ingestion_date)
                                                     FROM weather_processed.weather_data)
                             """)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create a function to update the *weather_presentation.weather_report* table

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

merge_condition = "tgt.city_name = src.city_name AND tgt.reported_date = src.reported_date"
presentation_folder_path = "/mnt/weatherprojectdl/presentation"
merge_delta_data(incoming_data_df,"weather_presentation","weather_report",presentation_folder_path,merge_condition)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Query to validate that the number of records equals the expected number of records

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATEDIFF(NOW(),'1990-01-01')*264 == COUNT(*)  as expected_records_match, 
# MAGIC MAX(reported_date) as data_max_date
# MAGIC from weather_presentation.weather_report

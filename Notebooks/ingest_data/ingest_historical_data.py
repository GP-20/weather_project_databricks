# Databricks notebook source
# MAGIC %md
# MAGIC ##### Read parquet file with historical data

# COMMAND ----------

historical_data_df = spark.read.parquet("/mnt/weatherprojectdl/historical-data/historical_data.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Rename columns

# COMMAND ----------

renamed_df = historical_data_df.withColumnRenamed("hourly.time","date") \
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
# MAGIC ##### Save the dataframe as a table in delta format

# COMMAND ----------

final_df.write.mode("overwrite").format("delta").saveAsTable("weather_processed.weather_data")

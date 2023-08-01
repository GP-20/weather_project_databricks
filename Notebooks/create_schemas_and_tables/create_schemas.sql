-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS weather_processed
LOCATION "/mnt/weatherprojectdl/processed"

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS weather_presentation
LOCATION "/mnt/weatherprojectdl/presentation"

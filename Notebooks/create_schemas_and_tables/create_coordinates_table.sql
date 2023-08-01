-- Databricks notebook source
DROP TABLE IF EXISTS weather_processed.coordinates;

CREATE TABLE IF NOT EXISTS weather_processed.coordinates(
  latitude DOUBLE,
  longitude DOUBLE,
  city_name STRING
)
USING json
OPTIONS (path "/mnt/weatherprojectdl/coordinates/geographical_coordinates.json", multiLine true)

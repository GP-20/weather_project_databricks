-- Databricks notebook source
CREATE TABLE IF NOT EXISTS weather_presentation.weather_report
AS
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

-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Units of each variable
-- MAGIC - **temperature:** °C
-- MAGIC - **humidity:** %
-- MAGIC - **rain:** mm
-- MAGIC - **snowfall:** cm
-- MAGIC - **wind speed:** km/h
-- MAGIC - **reported_date:** timestamp corresponding to local timezone

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Monthly average temperature, humidity, rain, snowfall and wind speed from 1990 to this date, per city

-- COMMAND ----------

SELECT city_name,
      DATE_FORMAT(reported_date,'MMMM' ) AS month,
      ROUND(AVG(temperature),2) AS average_temperature,
      ROUND(AVG(humidity),2) AS average_humidity,
      ROUND(AVG(rain),2) AS average_rain,
      ROUND(AVG(snowfall),5) AS average_snowfall,
      ROUND(AVG(wind_speed),2) AS average_wind_speed
FROM weather_presentation.weather_report
GROUP BY 1,2
ORDER BY 1,TO_DATE(month,'MMMM')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Maximum temperature, humidity, rain, snowfall and wind speed from 1990 to this date per city

-- COMMAND ----------

SELECT city_name,
      MAX(temperature) AS max_temperature,
      MAX(humidity) AS max_humidity,
      MAX(rain) AS max_rain,
      MAX(snowfall) AS max_snowfall,
      MAX(wind_speed) AS max_wind_speed
      FROM weather_presentation.weather_report
      GROUP BY 1
      ORDER BY 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Reported date of the maximum temperature of each city

-- COMMAND ----------

SELECT m.city_name,
       m.temperature,
       m.reported_date
FROM weather_presentation.weather_report m
JOIN (SELECT city_name,
      MAX(temperature) AS max_temperature
      FROM weather_presentation.weather_report
      GROUP BY 1) sub
ON m.city_name = sub.city_name AND m.temperature = sub.max_temperature
ORDER BY 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Reported day with the highest average temperature of each city

-- COMMAND ----------

SELECT city_name,
       day_date AS reported_date,
       ROUND(average_temperature_per_day,2) AS average_temperature_per_day
FROM (SELECT city_name,
      DATE_TRUNC('DAY',reported_date) AS day_date,
      AVG(temperature) AS average_temperature_per_day,
      RANK() OVER (PARTITION BY city_name ORDER BY AVG(temperature) DESC) AS temperature_rank
      FROM weather_presentation.weather_report
      GROUP BY 1,2)
WHERE temperature_rank = 1
ORDER BY 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Reported date of the maximum rain of each city

-- COMMAND ----------

SELECT m.city_name,
       m.rain,
       m.reported_date
FROM weather_presentation.weather_report m
JOIN (SELECT city_name,
      MAX(rain) AS max_rain
      FROM weather_presentation.weather_report
      GROUP BY 1) sub
ON m.city_name = sub.city_name AND m.rain = sub.max_rain
ORDER BY 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Reported day with the most rain of each city

-- COMMAND ----------

SELECT city_name,
       day_date AS reported_date,
       ROUND(rain_per_day,2) AS rain_per_day
FROM (SELECT city_name,
      DATE_TRUNC('DAY',reported_date) AS day_date,
      SUM(rain) AS rain_per_day,
      RANK() OVER (PARTITION BY city_name ORDER BY SUM(rain) DESC) AS rain_rank
      FROM weather_presentation.weather_report
      GROUP BY 1,2)
WHERE rain_rank = 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Minimum temperature and humidity per city

-- COMMAND ----------

SELECT city_name,
      MIN(temperature) AS min_temperature,
      MIN(humidity) AS min_humidity
      FROM weather_presentation.weather_report
      GROUP BY 1
      ORDER BY 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Reported date of the minimum temperature of each city

-- COMMAND ----------

SELECT m.city_name,
       m.temperature,
       m.reported_date
FROM weather_presentation.weather_report m
JOIN (SELECT city_name,
      MIN(temperature) AS min_temperature
      FROM weather_presentation.weather_report
      GROUP BY 1) sub
ON m.city_name = sub.city_name AND m.temperature = sub.min_temperature
ORDER BY 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Reported temperatures in the last seven days 

-- COMMAND ----------

SELECT city_name,
       reported_date,
       temperature
FROM weather_presentation.weather_report
WHERE reported_date >= DATE_SUB(NOW(), 7) AND city_name IN ('ciudad juarez','tijuana','hermosillo','monterrey')
ORDER BY 1,2

-- COMMAND ----------

SELECT city_name,
       max(temperature),
       min(temperature)
FROM weather_presentation.weather_report
WHERE reported_date >= (SELECT max(DATE_TRUNC('DAY',reported_date)) FROM weather_presentation.weather_report)
GROUP BY 1

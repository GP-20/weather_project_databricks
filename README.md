# Weather Project 

## Introduction

Weather Project is a weather monitoring tool for 11 cities in Mexico. This personal project showcases my abilities in data engineering tools. The pipeline is orchestrated using Azure Data Factory to gather weather data from the [Open-Meteo API](https://open-meteo.com/en/features#available-apis) for each city. The data is then stored as JSON files in Azure Data Lake Storage. Subsequently, the files are fed into Databricks for data transformation processes, generating a final report table that allows users to query the data. Finally, a [Python script](https://github.com/GP-20/weather_project_databricks/blob/main/Notebooks/reports/weather_graphs.py) runs to create graphs related to the local temperatures during the past week. The entire process runs daily at 08:00 UTC using a scheduled trigger.

## Table of Contents

- [Introduction](#introduction)
- [Technologies Used](#technologies-used)
- [Features](#features)
- [Generated graphs](#generated-graphs)

## Technologies Used

- Azure Data Factory - [Azure Data Factory repo](https://github.com/GP-20/weather_project_pipeline/tree/main)
- Azure Data Lake Storage
- Databricks
- Python
- SQL

## Features

- Fetch weather data from [Open-Meteo API](https://open-meteo.com/en/features#available-apis) for the following cities in Mexico:
  - Cancun
  - Ciudad Juarez
  - Guadalajara
  - Hermosillo
  - Merida
  - Mexico city
  - Monterrey
  - Queretaro
  - Tijuana
  - Tuxtla Gutierrez
  - Veracruz
- Store the weather data as JSON files in Azure Data Lake Storage.
- Perform data transformation processes using Databricks.
- Generate a final report table for querying the weather data.
- Create graphs related to the local temperatures during the past week using Python.

## Generated Graphs

#### Maximum and Minimum Temperatures
![max and min](https://raw.githubusercontent.com/GP-20/weather_project_databricks/main/Notebooks/reports/images/max_and_min.jpeg)
#### 7-Day Hourly Temperature History
![7-day-n](https://raw.githubusercontent.com/GP-20/weather_project_databricks/main/Notebooks/reports/images/hourly_north.png)
![7-day-c](https://raw.githubusercontent.com/GP-20/weather_project_databricks/main/Notebooks/reports/images/hourly_center.png)
![7-day-s](https://raw.githubusercontent.com/GP-20/weather_project_databricks/main/Notebooks/reports/images/hourly_south.png)
#### Yearly Average Temperature
![yearly](https://raw.githubusercontent.com/GP-20/weather_project_databricks/main/Notebooks/reports/images/yearly_average.png)

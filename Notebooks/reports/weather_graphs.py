# Databricks notebook source
# MAGIC %md
# MAGIC ### Yearly average temperature per city

# COMMAND ----------

plot_df = spark.sql("""SELECT city_name,
      DATE_TRUNC('YEAR',reported_date) AS day_date,
      ROUND(AVG(temperature),2) AS average_temperature_per_year
FROM weather_presentation.weather_report
GROUP BY 1,2
ORDER BY 1,2""").toPandas()

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timedelta

# COMMAND ----------

colors = {"cancun":"#0C134F","ciudad juarez":"#f86F03",
         "guadalajara":"#4aac7d","hermosillo":"#F4D160",
         "merida":"#FF78C4","mexico city":"#c992bf",
         "monterrey":"#cc3494","queretaro":"#9BE8D8",
         "tijuana":"#dab386","tuxtla gutierrez":"#ec1558",
         "veracruz":"#0ec1d7"}

# COMMAND ----------

fig, ax = plt.subplots(figsize=(15,8))
fig.set_facecolor("#ffffff")
ax.set_facecolor('#ffffff')
ax.set_ylim([12, 30])
ax.set_ylabel("°C",size=12)
ax.set_title("Yearly average temperature per city",size=15)
for key, grp in plot_df.groupby(['city_name']):
    ax = grp.plot(ax=ax, kind='line', x='day_date', y='average_temperature_per_year', color= colors[key],label=key,linestyle='--',marker='o')
    ax.set_xlabel("Year",size=12)
    ax.set_xlim([19.5, 54])

ax.legend(loc='center left', bbox_to_anchor=(1.1, 0.5),fontsize=12)
plt.grid()
plt.savefig('/Workspace/Repos/gdata20@outlook.com/weather_project_databricks/Notebooks/reports/images/yearly_average.png', bbox_inches='tight')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Hourly temperature per city in the last 7 days

# COMMAND ----------

plot_df_daily_north = spark.sql("""SELECT city_name,
       reported_date,
       temperature
FROM weather_presentation.weather_report
WHERE reported_date >= DATE_SUB(NOW(), 7) AND city_name IN ('ciudad juarez','tijuana','hermosillo','monterrey')
ORDER BY 1,2""").toPandas() 

# COMMAND ----------

fig, ax = plt.subplots(figsize=(15,8))
fig.set_facecolor("#ffffff")
ax.set_facecolor('#ffffff')
#ax.set_ylim([12, 30])
ax.set_ylabel("°C",size=12)
ax.set_title("Hourly temperature per city in the last 7 days",size=15)
for key, grp in plot_df_daily_north.groupby(['city_name']):
    ax = grp.plot(ax=ax, kind='line', x='reported_date', y='temperature', color= colors[key],label=key,linestyle='--',marker='o')
    ax.set_xlabel("Date",size=12)

ax.legend(loc='center left', bbox_to_anchor=(1.1, 0.5),fontsize=12)
plt.grid()
plt.savefig('/Workspace/Repos/gdata20@outlook.com/weather_project_databricks/Notebooks/reports/images/hourly_north.png', bbox_inches='tight')



# COMMAND ----------

plot_df_daily_center = spark.sql("""SELECT city_name,
       reported_date,
       temperature
FROM weather_presentation.weather_report
WHERE reported_date >= DATE_SUB(NOW(), 7) AND city_name IN ('guadalajara','veracruz','queretaro','mexico city')
ORDER BY 1,2""").toPandas() 

# COMMAND ----------

fig, ax = plt.subplots(figsize=(15,8))
fig.set_facecolor("#ffffff")
ax.set_facecolor('#ffffff')
#ax.set_ylim([12, 30])
ax.set_ylabel("°C",size=12)
ax.set_title("Hourly temperature per city in the last 7 days",size=15)
for key, grp in plot_df_daily_center.groupby(['city_name']):
    ax = grp.plot(ax=ax, kind='line', x='reported_date', y='temperature', color= colors[key],label=key,linestyle='--',marker='o')
    ax.set_xlabel("Date",size=12)

ax.legend(loc='center left', bbox_to_anchor=(1.1, 0.5),fontsize=12)
plt.grid()
plt.savefig('/Workspace/Repos/gdata20@outlook.com/weather_project_databricks/Notebooks/reports/images/hourly_center.png', bbox_inches='tight')

# COMMAND ----------

plot_df_daily_south = spark.sql("""SELECT city_name,
       reported_date,
       temperature
FROM weather_presentation.weather_report
WHERE reported_date >= DATE_SUB(NOW(), 7) AND city_name IN ('cancun','tuxtla gutierrez','merida')
ORDER BY 1,2""").toPandas() 

# COMMAND ----------

fig, ax = plt.subplots(figsize=(15,8))
fig.set_facecolor("#ffffff")
ax.set_facecolor('#ffffff')
#ax.set_ylim([12, 30])
ax.set_ylabel("°C",size=12)
ax.set_title("Hourly temperature per city in the last 7 days",size=15)
for key, grp in plot_df_daily_south.groupby(['city_name']):
    ax = grp.plot(ax=ax, kind='line', x='reported_date', y='temperature', color= colors[key],label=key,linestyle='--',marker='o')
    ax.set_xlabel("Date",size=12)

ax.legend(loc='center left', bbox_to_anchor=(1.1, 0.5),fontsize=12)
plt.grid()
plt.savefig('/Workspace/Repos/gdata20@outlook.com/weather_project_databricks/Notebooks/reports/images/hourly_south.png', bbox_inches='tight')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Maximum and minimum temperatures of the day

# COMMAND ----------

plot_temperatures_df = spark.sql("""
                             SELECT city_name,
                                    MAX(temperature) AS max_temperature,
                                    MIN(temperature) AS min_temperature
                                FROM weather_presentation.weather_report
                                WHERE reported_date >= (SELECT MAX(DATE_TRUNC('DAY',reported_date)) FROM weather_presentation.weather_report)
                                GROUP BY 1
                                ORDER BY 1
                             """).toPandas()

# COMMAND ----------

cities = plot_temperatures_df["city_name"].tolist()
Maximum_temperature = plot_temperatures_df["max_temperature"].tolist()
Minimum_temperature = plot_temperatures_df["min_temperature"].tolist()

x = np.arange(len(cities))
width = 0.25  

fig, ax = plt.subplots(figsize=(15,8))
max_t = ax.bar(x,Maximum_temperature,width,color="#FF0060",label='Maximum temperature')
min_t =ax.bar(x+0.5,Minimum_temperature,width,color="#0079FF",label='Minimum temperature')
ax.bar_label(max_t, padding=3)
ax.bar_label(min_t, padding=3)
ax.set_ylabel("°C",size=12)
title_day = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
ax.set_title(f'Maximum and minimum temperatures on {title_day}',size=15)
ax.set_xticks(x + width, cities)
ax.legend(loc='center left', bbox_to_anchor=(1.1, 0.5),fontsize=12)
plt.savefig('/Workspace/Repos/gdata20@outlook.com/weather_project_databricks/Notebooks/reports/images/max_and_min.jpeg', bbox_inches='tight')

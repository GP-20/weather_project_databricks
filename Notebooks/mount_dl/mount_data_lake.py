# Databricks notebook source
# MAGIC %md
# MAGIC ####Create function to mount the data lake containers

# COMMAND ----------

def mount_dl(storage,container):
    #Set secrets
    client_id=dbutils.secrets.get(scope="weather-scope" , key="client-id")
    tenant_id=dbutils.secrets.get(scope="weather-scope", key="tenant-id")
    client_secret=dbutils.secrets.get(scope="weather-scope", key="client-secret")
    #Set Spark Configuration
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    #Unmount if mount already exists
    if any(mount.mountPoint == f"/mnt/{storage}/{container}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage}/{container}")

    dbutils.fs.mount(
    source = f"abfss://{container}@{storage}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage}/{container}",
    extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount the raw, processed, presentation, historical-data and coordinates containers

# COMMAND ----------

mount_dl("weatherprojectdl","raw")

# COMMAND ----------

mount_dl("weatherprojectdl","processed")

# COMMAND ----------

mount_dl("weatherprojectdl","presentation")

# COMMAND ----------

mount_dl("weatherprojectdl","historical-data")

# COMMAND ----------

mount_dl("weatherprojectdl","coordinates")

# COMMAND ----------

display(dbutils.fs.mounts())

# Databricks notebook source
dbutils.secrets.help()

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("project_scope")

# COMMAND ----------

storage_account_name = "bigdatastorage10"
client_id = dbutils.secrets.get(scope= "project_scope", key= "databricks-client-id")
tenant_id = dbutils.secrets.get(scope= "project_scope", key= "databricks-tenant-id")
client_secret = dbutils.secrets.get(scope= "project_scope", key= "databricks-client-secret")



# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

container_name = "raw"
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/{container_name}",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/bigdatastorage10/raw")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

container_name = "processed"
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/{container_name}",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/bigdatastorage10/processed")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

def mount_adls(container_name):
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)

# COMMAND ----------

mount_adls("raw")
mount_adls("processed")

# COMMAND ----------

mount_adls("presentation")

# COMMAND ----------

# dbutils.fs.unmount("/mnt/bigdatastorage10/raw")

# COMMAND ----------

# dbutils.fs.unmount("/mnt/bigdatastorage10/processed")

# COMMAND ----------

dbutils.fs.ls("/mnt/bigdatastorage10/")

# COMMAND ----------


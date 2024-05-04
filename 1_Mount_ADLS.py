# Databricks notebook source
storage_account_name = "scdatabricks24"
container_name = "dbfs-container"
mount_point = "ADLS_Mount"
client_id = "b4cd0cce-e5c0-4ae2-be88-d7e25e523709"
tenant_id = "2e1f2451-fa12-4ba9-8ccf-679cbe31d4ea"
client_secret = ""

# COMMAND ----------

#https://learn.microsoft.com/en-us/azure/databricks/dbfs/mounts
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{client_id}",
          "fs.azure.account.oauth2.client.secret": f"{client_secret}",
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{mount_point}",
  extra_configs = configs)

# COMMAND ----------

configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsAzureADAuthenticator",
  "fs.azure.account.oauth2.client.id": f"{client_id}",
  "fs.azure.account.oauth2.client.secret": f"{client_secret}",
  "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
  "fs.azure.createRemoteFileSystemDuringInitialization": "true"
}



# COMMAND ----------

dbutils.fs.mount(
  source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point=f"/mnt/{mount_point}",
  extra_configs=configs
)

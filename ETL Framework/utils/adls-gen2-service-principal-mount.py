# Databricks notebook source
# Python code to mount and access Azure Data Lake Storage Gen2 Account from Azure Databricks with Service Principal and OAuth
 
# Define the variables used for creating connection strings
adlsAccountName = "azrawdatalakeglsah"
adlsContainerName = "raw"
# adlsFolderName = "data"
mountPoint = "/mnt/raw"
 
# Application (Client) ID
applicationId = dbutils.secrets.get(scope="key-vault-secrets",key="ClientId")
 
# Application (Client) Secret Key
authenticationKey = dbutils.secrets.get(scope="key-vault-secrets",key="service-credential-key-name")
 
# Directory (Tenant) ID
tenandId = dbutils.secrets.get(scope="key-vault-secrets",key="TenantId")
 
endpoint = "https://login.microsoftonline.com/" + tenandId + "/oauth2/token"
source = "abfss://" + adlsContainerName + "@" + adlsAccountName + ".dfs.core.windows.net/"# + adlsFolderName
 
# Connecting using Service Principal secrets and OAuth
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": applicationId,
           "fs.azure.account.oauth2.client.secret": authenticationKey,
           "fs.azure.account.oauth2.client.endpoint": endpoint}
 
# Mount ADLS Storage to DBFS only if the directory is not already mounted
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  dbutils.fs.mount(
    source = source,
    mount_point = mountPoint,
    extra_configs = configs)

# COMMAND ----------

# Python code to mount and access Azure Data Lake Storage Gen2 Account from Azure Databricks with Service Principal and OAuth
 
# Define the variables used for creating connection strings
adlsAccountName = "azrawdatalakeglsah"
adlsContainerName = "bronze"
# adlsFolderName = "data"
mountPoint = "/mnt/bronze"
 
# Application (Client) ID
applicationId = dbutils.secrets.get(scope="key-vault-secrets",key="ClientId")
 
# Application (Client) Secret Key
authenticationKey = dbutils.secrets.get(scope="key-vault-secrets",key="service-credential-key-name")
 
# Directory (Tenant) ID
tenandId = dbutils.secrets.get(scope="key-vault-secrets",key="TenantId")
 
endpoint = "https://login.microsoftonline.com/" + tenandId + "/oauth2/token"
source = "abfss://" + adlsContainerName + "@" + adlsAccountName + ".dfs.core.windows.net/"# + adlsFolderName
 
# Connecting using Service Principal secrets and OAuth
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": applicationId,
           "fs.azure.account.oauth2.client.secret": authenticationKey,
           "fs.azure.account.oauth2.client.endpoint": endpoint}
 
# Mount ADLS Storage to DBFS only if the directory is not already mounted
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  dbutils.fs.mount(
    source = source,
    mount_point = mountPoint,
    extra_configs = configs)

# COMMAND ----------

dbutils.fs.unmount("/mnt/raw")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE bronze;
# MAGIC CREATE DATABASE silver;
# MAGIC CREATE DATABASE gold;

# Databricks notebook source
# MAGIC %md
# MAGIC *PLEASE CLONE THIS NOTEBOOK TO TEST IT*

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Azure Data Lake Storage (ADLS) and Databricks
# MAGIC 
# MAGIC <img src="https://oneenvstorage.blob.core.windows.net/images/adlslogo.png" width="400">
# MAGIC 
# MAGIC | Specs                                                    |
# MAGIC |----------------------|-----------------------------------|
# MAGIC |Azure Resource Group         | oneenv                     |
# MAGIC |ADLS Account         | oneenvadls                      |
# MAGIC | Containers         | deltalake, files                        |
# MAGIC | Region         | US West| 
# MAGIC 
# MAGIC </br>
# MAGIC 1. Use SAS Key
# MAGIC 2. Use the Azure Key Vault backed secrets scope
# MAGIC 
# MAGIC Docs: https://docs.databricks.com/data/data-sources/azure/azure-datalake-gen2.html

# COMMAND ----------

# MAGIC %sh /databricks/conda/envs/databricks-ml/bin/databricks configure

# COMMAND ----------

# MAGIC %sh databricks secrets create-scope --scope RWERenewablesDemoScope

# COMMAND ----------

import requests
API_URL = "https://eastus2.azuredatabricks.net/api/2.0/secrets/scopes/create"
dbc_token = "dapib9501749ad36eddee2c97e4e48dde648"

api_data = """{
  "scope": "RWERenewablesScope",
  "initial_manage_principal": "users"
} """

queryUrl = API_URL.format(API_URL)
print(queryUrl)

myResponse = requests.post(queryUrl, headers={'Authorization': 'Bearer {0}'.format(dbc_token)}, data = api_data)  
print(myResponse.status_code)

# For successful API call, response code will be 200 (OK)
if(myResponse.ok):

    print("call returned OK")
    print(myResponse.content)
else:
    print(myResponse.raise_for_status())

# COMMAND ----------

import requests
API_URL = "https://eastus2.azuredatabricks.net/api/2.0/secrets/put"
dbc_token = "dapib9501749ad36eddee2c97e4e48dde648"

APP_KEY = """{
  "scope": "RWERenewablesScope",
  "key": "RWERenewablesScopeKey",
  "string_value": "q7JwBZ:uF-f6.ls-PJ5AhwBy97[rtY57"
}"""

queryUrl = API_URL.format(API_URL)
print(queryUrl)

myResponse = requests.post(queryUrl, headers={'Authorization': 'Bearer {0}'.format(dbc_token)}, data = APP_KEY)  

# For successful API call, response code will be 200 (OK)
if(myResponse.ok):

    print("call returned OK")
    print(myResponse.content)
else:
    print(myResponse.raise_for_status())

# COMMAND ----------

dbutils.fs.unmount(MOUNT_POINT)

# COMMAND ----------

# This only needs to be ran once, globably. Once we have mounted the storage account no need need to do it again (unless you unmount). 


STORAGE_ACCOUNT = "oneenvadls"
CONTAINER = "deltalake"
MOUNT_POINT = "/mnt/rwe_renewables/demo"

# The below details are related to the Service Principal oneenv-adls
APPLICATION_ID = "ed573937-9c53-4ed6-b016-929e765443eb"
DIRECTORY_ID = "9f37a392-f0ae-4280-9796-f1864a10effc"
# You can use the below if you don't want to use key vault
APP_KEY = dbutils.secrets.get(scope = "RWERenewablesScope", key = "RWERenewablesScopeKey")

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": APPLICATION_ID,
           "fs.azure.account.oauth2.client.secret": APP_KEY,
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/"+DIRECTORY_ID+"/oauth2/token"}
#Run the mount function using template in the documentation
try:
  dbutils.fs.mount(
  source = "abfss://"+CONTAINER+"@"+STORAGE_ACCOUNT+".dfs.core.windows.net/",
  mount_point = MOUNT_POINT,
  extra_configs = configs)
  
except Exception as e:
  print("ERROR: {} ".format(e))
  
#If needed to unmount, use this:
#try:
#  dbutils.fs.unmount(MOUNT_POINT) # Use this to unmount as needed
#except:
#  print("{} already unmounted".format(MOUNT_POINT))

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/rwe_renewables/demo
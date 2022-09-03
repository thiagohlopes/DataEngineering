# Databricks notebook source
# MAGIC %md
# MAGIC #Realizando os imports

# COMMAND ----------

import requests
from requests.exceptions import HTTPError
import pandas as pd
from json import dumps
import requests
from pyspark.sql import *
from pyspark.dbutils import *

# COMMAND ----------

file_location = "/FileStore/tables/california_housing_train.csv"
CA_housing = readCsvWriteSpkDf(file_location)
folder_path = "california_housing/"
copyToLandingLayer(folder_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #Lendo csv e transformando em DF

# COMMAND ----------

def readCsvWriteSpkDf(file_location):
    infer_schema = "true"
    first_row_is_header = "true"
    delimiter = ","

    try:
        spark_df = spark.read.format("csv") \
          .option("inferSchema", infer_schema) \
          .option("header", first_row_is_header) \
          .option("sep", delimiter) \
          .load(file_location)

        display(spark_df)
    except Exception as e:       
        errorChatMessage(f'ERROR: readCsvWriteSpkDf para {file_location}. \n Err: {e}')
    else:
        return spark_df

# COMMAND ----------

# MAGIC %md
# MAGIC #Realziando a copia do arquivo DF para a camada Landing em formato parquet

# COMMAND ----------

def copyToLandingLayer(folder_path):
    try:
        
        container_name = "landing"
        
        
#         storage_account_name = dbutils.secrets.get(scope = "maistodos-data-team", key = 'storage_account_name')
        storage_account_name="maistodosdatalake"

#         storage_account_key = dbutils.secrets.get(scope = "maistodos-data-team", key = 'storage_account_key')
        storage_account_key= "maistodosdatalakepassword"

        spark.conf.set(
            f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
            f"{storage_account_key}")

        df_storage_batch_raw.write.format("parquet").mode("overwrite").save(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}")

    except Exception as e:
        chatMessage.incrementErrors()        
        errorChatMessage(f'ERROR: copyToLandingLayer no {folder_path}. \n Err: {e}')
    else:
        chatMessage.addSuccessMessage(f'SUCCESSS: Cópia de {folder_path} para camada landing foi realizada com sucesso!')
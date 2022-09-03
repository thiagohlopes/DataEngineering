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

# MAGIC %md
# MAGIC #Lendo csv e transformando em DF

# COMMAND ----------

def connectToStorage():
#         storage_account_name = dbutils.secrets.get(scope = "maistodos-data-team", key = 'storage_account_name')
        storage_account_name="maistodosdatalake"

#         storage_account_key = dbutils.secrets.get(scope = "maistodos-data-team", key = 'storage_account_key')
        storage_account_key= "maistodosdatalakepassword"

        spark.conf.set(
            f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
            f"{storage_account_key}")

# COMMAND ----------

# MAGIC %md
# MAGIC #Realziando a copia da landing para a camada bronze e criando tabela delta

# COMMAND ----------

def copyToBronzeLayer(folder_path):
    try:
        container_name = "landing"
        
#         storage_account_name = dbutils.secrets.get(scope = "maistodos-data-team", key = 'storage_account_name')
        storage_account_name="maistodosdatalake"

#         storage_account_key = dbutils.secrets.get(scope = "maistodos-data-team", key = 'storage_account_key')
        storage_account_key= "maistodosdatalakepassword"

        spark.conf.set(
            f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
            f"{storage_account_key}")

        df_landing = spark.read.format("parquet")\
        .option("header", "true").option("inferSchema","true") \
        .load(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}")

    except Exception as e:   
        print(f'ERROR: copyToDataLake no {folder_path}. \n Err: {e}')
    else:
        try: 
            mergeDeltaTable(container_name, storage_account_name, folder_path)
        except Exception as e:
            if ("not a Delta table" in str(e)) or("Incompatible format detected" in str(e)):
                firstBatchToDeltaTable(container_name, storage_account_name, folder_path)
            else: 
                print(f'ERROR: copyToDataLake no {folder_path}. \n Err: {e}')
    return df_landing
    

# COMMAND ----------

def mergeDeltaTable(container_name, storage_account_name, folder_path):
    
    new = spark.read.format("parquet").load(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}")

    bronze = spark.read.format("delta").load(f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/{folder_path}")
    
    # add columns in dataframe1 that are missing from dataframe2
    for column in [column for column in new.columns
                   if column not in bronze.columns]:
        bronze = bronze.withColumn(column, lit(None))

    # add columns in dataframe2 that are missing from dataframe1
    for column in [column for column in bronze.columns
                   if column not in new.columns]:
        new = new.withColumn(column, lit(None))
    
    deltaTable = DeltaTable.forPath(spark, f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/{folder_path}")
    
    (deltaTable
     .alias("bronze") 
     .merge(new.alias("newData"), "bronze.id = newData.id")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute()
     )

    deltaTable.toDF().show()

# COMMAND ----------

def firstBatchToDeltaTable(container_name, storage_account_name, folder_path):
    
    try: 
        
        new = spark.read.format("parquet").load(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}")
        new.write.format("delta").mode("append").save(f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/{folder_path}")
        
        spark.sql("create database if not exists bronze")
        folder = folder_path.replace("/","_")

        ddl_query = f"""CREATE TABLE if not exists bronze.{folder}
                           USING DELTA
                           LOCATION 'abfss://bronze@{storage_account_name}.dfs.core.windows.net/{folder_path}'
                           """
        
        ddl2 = f"""ALTER TABLE bronze.{folder} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)""";
    
        spark.sql(ddl_query)
        spark.sql(ddl2)

        bronze = spark.read.format("delta").load(f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/{folder_path}")
        print(bronze.show())
    except Exception as e:  
        print(f'ERROR: firstBatchToDeltaTable no Batch {folder_path}. \n Err: {e}')


# COMMAND ----------

folder_path = "california_housing/"
copyToBronzeLayer(folder_path)

# COMMAND ----------

# def createBronzeDeltaTable(folder_path):
#         storage_account_name = dbutils.secrets.get(scope = "maistodos-data-team", key = 'storage_account_name')

#     bronze = spark.read.format("delta").load(f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/{folder_path}")

#     spark.sql("create database if not exists bronze")

#     ddl_query = f"""CREATE TABLE if not exists bronze.{folder}
#                        USING DELTA
#                        LOCATION 'abfss://bronze@{storage_account_name}.dfs.core.windows.net/{folder_path}'
#                        """

#     ddl2 = f"""ALTER TABLE bronze.{folder} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)""";

#     spark.sql(ddl_query)
#     spark.sql(ddl2)
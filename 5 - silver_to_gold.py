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
# MAGIC #Função para realizar conexão com storage, mas como é uma simulação não usei a função para ter controle de onde a conexao estava sendo feita

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
# MAGIC #Fazendo as transformações do DF

# COMMAND ----------

def transformDataFrame(df):
    df = df.groupBy("age", "california_region").agg(sum("population").alias("s_population"), avg("median_house_value").alias("m_median_house_value"))
    
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC #Mergeando apenas as mudanças que ocorreram no ultimo dia

# COMMAND ----------

def firstMergegoldLayer(storage_account_name, folder_path):
    
    try: 
        
        new = spark.read.format("parquet").load(f"abfss://gold@{storage_account_name}.dfs.core.windows.net/{folder_path}")
        new.write.format("delta").mode("append").save(f"abfss://silver@{storage_account_name}.dfs.core.windows.net/{folder_path}")
        
        spark.sql("create database if not exists silver")
        folder = folder_path.replace("/","_")

        ddl_query = f"""CREATE TABLE if not exists silver.{folder}
                           USING DELTA
                           LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/{folder_path}'
                           """
        
        ddl2 = f"""ALTER TABLE silver.{folder} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)""";
    
        spark.sql(ddl_query)
        spark.sql(ddl2)

    except Exception as e:  
        print(f'ERROR: firstMergegoldLayer no Batch {folder_path}. \n Err: {e}')


# COMMAND ----------

 def mergegoldLayer(storage_account_name, folder_path):

    silver = spark.read.format("delta").option("readChangeFeed", "true").option("startingTimestamp", datetime.datetime.now().date()).load(f"abfss://silver@{storage_account_name}.dfs.core.windows.net/{folder_path}").filter(col("_change_type") != lit('update_preimage'))
    gold = spark.read.format("delta").load(f"abfss://gold@{storage_account_name}.dfs.core.windows.net/{folder_path}")
    silver = transformDataFrame(silver)
    
    # add columns in dataframe1 that are missing from dataframe2
    for column in [column for column in silver.columns
                   if column not in gold.columns]:
        gold = gold.withColumn(column, lit(None))

    # add columns in dataframe2 that are missing from dataframe1
    for column in [column for column in gold.columns
                   if column not in silver.columns]:
        silver = silver.withColumn(column, lit(None))
        
    deltaTable = DeltaTable.forPath(spark, f"abfss://gold@{storage_account_name}.dfs.core.windows.net/{folder_path}")
    
    (deltaTable
     .alias("gold") 
     .merge(silver.alias("silver"), "gold.id = silver.id")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute()
     )

    deltaTable.toDF().show()

# COMMAND ----------

# MAGIC %md
# MAGIC #Realizando a copia da silver para a camada gold e criando tabela delta

# COMMAND ----------

def copyTogold(folder_path):
    container_name = "silver"        
#     storage_account_name = dbutils.secrets.get(scope = "maistodos-data-team", key = 'storage_account_name')
    storage_account_name="maistodosdatalake"
#     storage_account_key = dbutils.secrets.get(scope = "maistodos-data-team", key = 'storage_account_key')
    storage_account_key= "maistodosdatalakepassword"
    try:
        if DeltaTable.isDeltaTable(spark, f"abfss://gold@{storage_account_name}.dfs.core.windows.net/{folder_path}"):
             print('a', DeltaTable.isDeltaTable(spark, f"abfss://gold@{storage_account_name}.dfs.core.windows.net/{folder_path}"))
             df_landing = mergegoldLayer(storage_account_name, folder_path)
        else:
            print('b', DeltaTable.isDeltaTable(spark, f"abfss://gold@{storage_account_name}.dfs.core.windows.net/{folder_path}"))
            df_landing = firstMergegoldLayer(storage_account_name, folder_path)
        
    except Exception as e:
        chatMessage.incrementErrors()     
        errorChatMessage(f'ERROR: copyTogold no Batch {path}. \n Err: {e}')
    else:
        chatMessage.incrementSuccesses()
        chatMessage.addSuccessMessage(f'SUCCESSS: Cópia no batchs {storage_path}, foi realizada com sucesso!')
        return df_landing      

   

# COMMAND ----------

# MAGIC %md
# MAGIC #Main

# COMMAND ----------

folder_path = "california_housing/"
copyTogold(folder_path)

# COMMAND ----------

# def createsilverDeltaTable(folder_path):
#         storage_account_name = dbutils.secrets.get(scope = "maistodos-data-team", key = 'storage_account_name')

#     silver = spark.read.format("delta").load(f"abfss://silver@{storage_account_name}.dfs.core.windows.net/{folder_path}")

#     spark.sql("create database if not exists silver")

#     ddl_query = f"""CREATE TABLE if not exists silver.{folder}
#                        USING DELTA
#                        LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/{folder_path}'
#                        """

#     ddl2 = f"""ALTER TABLE silver.{folder} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)""";

#     spark.sql(ddl_query)
#     spark.sql(ddl2)
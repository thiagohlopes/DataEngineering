# Databricks notebook source
# MAGIC %md
# MAGIC #Resumo do desafio MaisTodos com as transformações solicitadas
# MAGIC * Para realizar o teste deste programa é necessário realizar o upload do arquivo 
# MAGIC * data > create table > "drop the file in box"

# COMMAND ----------

# MAGIC %md
# MAGIC #Realizando os imports

# COMMAND ----------

import requests
from requests.exceptions import HTTPError
import pandas as pd
import numpy as np
from json import dumps
import requests
from pyspark.sql.functions import *
from pyspark.dbutils import *

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
    except Exception as e:       
        errorChatMessage(f'ERROR: readCsvWriteSpkDf para {file_location}. \n Err: {e}')
    else:
        return spark_df

# COMMAND ----------

# MAGIC %md
# MAGIC #Funções do topico 2
# MAGIC * Adicionando intervalo de dia

# COMMAND ----------

def addIntervalAge(df):
    df = df.withColumn('hma_cat', when(col('housing_median_age') < 18, "de_0_ate_18")
                  .when((col('housing_median_age') >= 18) & (col('housing_median_age') < 29), "ate_29")
                  .when((col('housing_median_age') >= 29) & (col('housing_median_age') < 37), "ate_37")
                  .otherwise("acima_37"))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC * Adicionando norte e sul

# COMMAND ----------

def definingCardinalPoints(df):
    df = df.withColumn('c_ns', when(col('longitude') < -119, "norte")
                  .otherwise("sul"))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC * Renomeando colunas

# COMMAND ----------

def renamingColumns(df):
    df = df.withColumnRenamed("hma_cat","age").withColumnRenamed("c_ns","california_region")
    return df

# COMMAND ----------

file_location = "/FileStore/tables/california_housing_train.csv"
CA_housing = readCsvWriteSpkDf(file_location)
print(CA_housing)

# COMMAND ----------

# MAGIC %md
# MAGIC #1 -Exploração

# COMMAND ----------

#1.1
exp11 = CA_housing
df = exp11.toPandas()
df = df.std()
df = df.index[df == df.max()]
print(df[0])

# COMMAND ----------

#1.2
exp12 = CA_housing
# #Max de cada coluna
#para pegar o max de cada coluna tirar 1 .max()
df = exp12.toPandas()
df = df.select_dtypes(include=[np.number]).max().max()
print("max: ",df)

#para pegar o min de cada coluna tirar 1 .min()
df = exp12.toPandas()
df = df.select_dtypes(include=[np.number]).min().min()
print("min: ",df)


# COMMAND ----------

# MAGIC %md
# MAGIC #2- Trabalhando com colunas

# COMMAND ----------

print("==========================================================")
print("Adicionando a coluna hma_cat")
CA_housing = addIntervalAge(CA_housing)
display(CA_housing)
print("==========================================================")
print("Adicionando a coluna c_ns")
CA_housing = definingCardinalPoints(CA_housing)
display(CA_housing)
print("==========================================================")
print("Renomeando as colunas hma_cat c_ns")
CA_housing = renamingColumns(CA_housing)
display(CA_housing)
print("==========================================================")

# COMMAND ----------

parquetFile = CA_housing.select("age","california_region", "total_rooms", "total_bedrooms", "population", "households", "median_house_value")
try:
    parquetFile.write.parquet("/tmp/raw/CA_housing.parquet") 
except Exception as e:  
        print(f'ERROR: {e}')
print("================================================================================")
print(parquetFile)
print("================================================================================")
display(parquetFile)

# COMMAND ----------

# MAGIC %md
# MAGIC #3- Agregações

# COMMAND ----------

newDf = CA_housing.groupBy("age", "california_region").agg(sum("population").alias("s_population"), avg("median_house_value").alias("m_median_house_value"))
print(newDf)
print("========================================")
display(newDf)
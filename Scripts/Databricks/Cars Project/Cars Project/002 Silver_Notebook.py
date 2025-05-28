# Databricks notebook source
# MAGIC %md
# MAGIC ### Read data from Bronze layer

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

df = spark.read.format('parquet')\
    .option('inferSchema' , True)\
    .load('abfss://bronze@caradedatalakemelvin.dfs.core.windows.net/rawdata/')

# COMMAND ----------

# df.display()
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q1. Add a new Column called Model_Category by taking the first word from the Model_ID column

# COMMAND ----------

df = df.withColumn('Model_Category',split(col('Model_ID'),'-')[0])
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Type casting an Integer column into a String.

# COMMAND ----------

df.withColumn('Units_Sold',col('Units_Sold').cast('String')).printSchema()
# df.withColumn('Units_Sold',col('Units_Sold').cast(StringType())).printSchema() 

# COMMAND ----------

df = df.withColumn('Revenue_Per_Unit',col('Revenue')/col('Units_Sold'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adhoc Analysis

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Find how many units sold for each branch per year

# COMMAND ----------

# df.groupBy('Year', 'BranchName').agg(sum('Units_Sold').alias('Total_Units')).orderBy('Year').display()
df.groupBy('Year', 'BranchName').agg(sum('Units_Sold').alias('Total_Units')).sort('Year','Total_Units',ascending=[True,False]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Data to Silver Layer

# COMMAND ----------

df.write.mode('overwrite')\
        .format('parquet')\
        .option('path','abfss://silver@caradedatalakemelvin.dfs.core.windows.net/carsales/')\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Querying Silver data

# COMMAND ----------

spark.read.format('parquet')\
    .option('inferSchema',True)\
    .load('abfss://silver@caradedatalakemelvin.dfs.core.windows.net/carsales/')\
    .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Querying a parquet file using sql command

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@caradedatalakemelvin.dfs.core.windows.net/carsales/`
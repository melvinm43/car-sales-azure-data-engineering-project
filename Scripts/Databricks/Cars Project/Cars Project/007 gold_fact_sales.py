# Databricks notebook source
# MAGIC %md
# MAGIC ### Create Fact Table

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading Silver Data**

# COMMAND ----------

df_silver = spark.sql('''
    select * from parquet.`abfss://silver@caradedatalakemelvin.dfs.core.windows.net/carsales/`
''')

# COMMAND ----------

df_silver.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading all dimesion tables as data frames for joining with fact table.**

# COMMAND ----------

df_model = spark.sql("select * from `cars_catalog`.`gold`.`dim_model`")
df_branch = spark.sql("select * from `cars_catalog`.`gold`.`dim_branch`")
df_dealer = spark.sql("select * from `cars_catalog`.`gold`.`dim_dealer`")
df_date = spark.sql("select * from `cars_catalog`.`gold`.`dim_date`")

# COMMAND ----------

# MAGIC %md
# MAGIC **Bringing Keys to the FACT table**

# COMMAND ----------

# MAGIC %md
# MAGIC Here in the below code left join and inner join act the same, as we load all the dimentions before the fact and all the foreign keys will be already present.

# COMMAND ----------

df_fact = df_silver.join(df_model,df_silver['Model_ID'] == df_model['Model_ID'],how='left')\
          .join(df_branch,df_silver['Branch_ID'] == df_branch['Branch_ID'],how='left')\
          .join(df_dealer,df_silver['Dealer_ID'] == df_dealer['Dealer_ID'],how='left')\
          .join(df_date,df_silver['Date_ID'] == df_date['Date_ID'],how='left')\
          .select(df_silver['Revenue'],df_silver['Units_Sold'],df_silver['Revenue_Per_Unit'],df_model['Model_Dim_Key'],df_branch['Branch_Dim_Key'],df_dealer['Dealer_Dim_Key'],df_date['Date_Dim_Key'])

# COMMAND ----------

df_fact.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Writing Fact Table**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

# Incremental Load logic
if spark.catalog.tableExists("cars_catalog.gold.fact_sales"):
    deltaTable = DeltaTable.forName(spark,"cars_catalog.gold.fact_sales")
    deltaTable.alias("trg").merge(df_fact.alias("src"),"trg.Dealer_Dim_Key = src.Dealer_Dim_Key AND trg.Model_Dim_Key = src.Model_Dim_Key AND trg.Branch_Dim_Key = src.Branch_Dim_Key AND trg.Date_Dim_Key = src.Date_Dim_Key")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
# Initial load logic
else:
    df_fact.write.format("delta")\
        .mode("overwrite")\
        .option('path','abfss://gold@caradedatalakemelvin.dfs.core.windows.net/fact_sales/')\
        .saveAsTable("cars_catalog.gold.fact_sales")
        


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.fact_sales;
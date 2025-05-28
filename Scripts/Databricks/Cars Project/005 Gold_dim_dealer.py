# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incremental Flag Setting

# COMMAND ----------

dbutils.widgets.dropdown("Incremental Flag", "0", ["0", "1"])
#text('Incremental Flag', '0')

# COMMAND ----------

incremental_flag = dbutils.widgets.get('Incremental Flag')
print(incremental_flag)
print(type(incremental_flag))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Dimesion dealer

# COMMAND ----------

df_source = spark.sql('''
select Distinct(Dealer_ID),DealerName from parquet.`abfss://silver@caradedatalakemelvin.dfs.core.windows.net/carsales`;
''')
df_source.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_dealer Sink , Initial and Incremental

# COMMAND ----------

if(spark.catalog.tableExists('cars_catalog.gold.dim_dealer')):
    df_sink = spark.sql('''
    select Dealer_Dim_Key,Dealer_ID,DealerName
    from
    cars_catalog.gold.dim_dealer;
    '''
    ) # To bring actual data for Joins
else:
    df_sink = spark.sql('''
    select 1 as Dealer_Dim_Key,Dealer_ID,DealerName
    from
    parquet.`abfss://silver@caradedatalakemelvin.dfs.core.windows.net/carsales`
    where 1=0;
    '''
    ) # To just get the schema


# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering Old and New Records
# MAGIC
# MAGIC By applying a left join from the sink/target table to the source table, we can identify records where the target table columns are null, indicating that these records are absent in the source table. <BR> If records are absent, they are new records and need to be inserted into the target table. <BR> Otherwise, the records are already present and should be updated.

# COMMAND ----------

df_filter = df_source.join(df_sink,df_source.Dealer_ID == df_sink.Dealer_ID,'left').select(df_source['Dealer_ID'],df_source['DealerName'],df_sink['Dealer_Dim_Key'])

# COMMAND ----------

df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **df_filter_old**

# COMMAND ----------

# Filter existing records in the table from the source
df_filter_old = df_filter.filter(col('Dealer_Dim_Key').isNotNull())

# COMMAND ----------

df_filter_old.display()

# COMMAND ----------

# Filter new records from the source and remove the surrogate key column as we already know that is always null
df_filter_new = df_filter.filter(col('Dealer_Dim_Key').isNull()).select(df_filter['Dealer_ID'],df_filter['DealerName'])

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating surrogate Key for Dimension Table
# MAGIC For initial run the surrogate key will be starting from 1. ( 0+1) <BR>
# MAGIC For Incremental runs , the surrogate key will be 1 + max(Model_Dim_Key)

# COMMAND ----------

# MAGIC %md
# MAGIC **Fetch the max surrogate key from the existing table**

# COMMAND ----------

if incremental_flag == '0':
    max_value = 1
else:
    max_value_df = spark.sql('select max(Dealer_Dim_Key) from cars_catalog.gold.dim_dealer')
    max_value = max_value_df.collect()[0][0] + 1

# What does the second line do?
# python
# Copy
# max_value = max_value_df.collect()[0][0]
# This:

# Triggers execution of the query (.collect())

# Returns the result as a list of rows (each row = Row(...))

# Takes the first row ([0])

# Takes the first column of that row ([0]), which is the result of MAX(Dealer_Dim_Key)

# COMMAND ----------

# MAGIC %md
# MAGIC **Create the surrogate key column and add the max surrogate key**

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('Dealer_Dim_Key',max_value+monotonically_increasing_id())

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Final Data frame - df_filter_old + df_filter_new

# COMMAND ----------

df_final = df_filter_old.union(df_filter_new)

# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##  SCD Type 1 - UPSERT

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

# Incremental Load
if spark.catalog.tableExists('cars_catelog.gold.dim_dealer'):
    delta_table = DeltaTable.forPath(spark,'abfss://gold@caradedatalakemelvin.dfs.core.windows.net/dim_dealer')
    delta_table.alias('target')\
                .merge(df_final.alias('source'),'target.Dealer_ID = source.Dealer_ID')\
                .whenMatchedUpdateAll()\
                .whenNotMatchedInsertAll()\
                .execute()

else:
    # Intial Load
    df_final.write.format('delta')\
        .mode('overwrite')\
        .option('path','abfss://gold@caradedatalakemelvin.dfs.core.windows.net/dim_dealer')\
        .saveAsTable('cars_catalog.gold.dim_dealer')

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `cars_catalog`; select * from `gold`.`dim_dealer` limit 100;
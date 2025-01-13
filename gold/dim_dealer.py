# Databricks notebook source
# MAGIC %md
# MAGIC creating a flag for tracking either its initial load or incremental

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------

dbutils.widgets.text('incremental_flag','0')

# COMMAND ----------

incremental_flag = dbutils.widgets.get('incremental_flag')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Dimension Model

# COMMAND ----------

src_df = spark.sql('''
                   SELECT DISTINCT(Dealer_ID) as Dealer_ID,DealerName 
                   FROM PARQUET.`abfss://silver@adlscarsales.dfs.core.windows.net`
                   ''')


# COMMAND ----------

## Brings schema if table doesn't exist
if spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):
    sink_df = spark.sql('''
                        SELECT Dim_Dealer_ID, Dealer_ID, DealerName
                        FROM cars_catalog.gold.dim_dealer
                        ''')

## Brings Dealer data
else:
        sink_df = spark.sql('''
                        SELECT 1 as Dim_Dealer_ID, Dealer_ID, DealerName
                        FROM PARQUET.`abfss://silver@adlscarsales.dfs.core.windows.net`
                        where 1=0
                        ''')

# COMMAND ----------

# MAGIC %md
# MAGIC LEFT JOIN ON SOURCE DATA

# COMMAND ----------

df_filter = src_df.join(sink_df,src_df['Dealer_ID'] == sink_df['Dealer_ID'], 'left').select(src_df['Dealer_ID'],src_df['DealerName'],sink_df['Dim_Dealer_ID'])

# COMMAND ----------

# MAGIC %md
# MAGIC SELECTING OLD DATA. OLD DATA ARE ONE WHERE Dim_Model_ID IS NOT NULL

# COMMAND ----------

df_filter_old = df_filter.filter(df_filter['Dim_Dealer_ID'].isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC SELECTING NEW DATA. NEW DATA ARE ONE WHERE Dim_Model_ID IS NULL

# COMMAND ----------

df_filter_new = df_filter.filter(df_filter['Dim_Dealer_ID'].isNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ### creating surrogate key

# COMMAND ----------

if (incremental_flag == '0'):
  max_value = 1

else:
  max_value = spark.sql("SELECT MAX(Dim_Dealer_ID) FROM cars_catalog.gold.dim_dealer")
  max_value = max_value.collect()[0][0]+1

# COMMAND ----------

# MAGIC %md
# MAGIC Create surrogate key column and add the max surrogate key

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('Dim_Dealer_ID', max_value + monotonically_increasing_id())

# COMMAND ----------

# MAGIC %md
# MAGIC create final df = df_filter_new + df_filter_old

# COMMAND ----------

df_final = df_filter_new.union(df_filter_old)

# COMMAND ----------

# MAGIC %md
# MAGIC applying slowly changing dimension type1 aka upsert

# COMMAND ----------

if not spark.catalog.tableExists('car_catalog.gold.dim_dealer'):
    df_final.write.format('delta')\
        .mode('overwrite')\
        .option('path','abfss://gold@adlscarsales.dfs.core.windows.net/dim_dealer')\
        .saveAsTable('cars_catalog.gold.dim_dealer')

else:
    delta_table = DeltaTable.forPath(spark, 'abfss://gold@adlscarsales.dfs.core.windows.net/dim_dealer')
    delta_table.alias('target').merge(df_final.alias('source'),'target.Dealer_ID = source.Dealer_ID')\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cars_catalog.gold.dim_dealer

# COMMAND ----------



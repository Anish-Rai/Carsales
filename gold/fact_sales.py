# Databricks notebook source
# MAGIC %md
# MAGIC Creating Fact table
# MAGIC

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

df_silver = spark.read.format('parquet').load('abfss://silver@adlscarsales.dfs.core.windows.net')

# COMMAND ----------

df_model = spark.sql("SELECT * FROM cars_catalog.gold.dim_model")

df_dealer = spark.sql("SELECT * FROM cars_catalog.gold.dim_dealer")

df_date = spark.sql("SELECT * FROM cars_catalog.gold.dim_date")

df_branch = spark.sql("SELECT * FROM cars_catalog.gold.dim_branch")

# COMMAND ----------

df_fact = df_silver.join(df_model, df_silver.Model_ID == df_model.Model_ID,how='left')\
        .join(df_dealer, df_silver.Dealer_ID == df_dealer.Dealer_ID,how='left')\
        .join(df_date, df_silver.Date_ID == df_date.Date_ID,how='left')\
        .join(df_branch, df_silver.Branch_ID == df_branch.Branch_ID,how='left')\
        .select(df_silver.Revenue, df_silver.Units_Sold, df_silver.RevPerUnit,df_branch.Dim_Branch_ID, df_dealer.Dim_Dealer_ID, df_date.Dim_Date_ID, df_model.Dim_Model_ID)

# COMMAND ----------

df_fact.display()

# COMMAND ----------

# Incremental load
if spark.catalog.tableExists("fact_sales"):
    delta_table = DeltaTable.forName(spark,'cars_catalog.gold.fact_sales')
    delta_table.alias('target').merge(
         df_fact.alias("source"),
        """
        target.Dim_Branch_ID = source.Dim_Branch_ID AND
        target.Dim_Dealer_ID = source.Dim_Dealer_ID AND
        target.Dim_Date_ID = source.Dim_Date_ID AND
        target.Dim_Model_ID = source.Dim_Model_ID
        """
        )\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
## Initial load
else:
    df_fact.write.format('delta')\
        .mode('overwrite')\
        .option('path','abfss://gold@adlscarsales.dfs.core.windows.net/fact_sales')\
        .saveAsTable('cars_catalog.gold.fact_sales')

# COMMAND ----------



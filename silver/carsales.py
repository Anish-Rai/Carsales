# Databricks notebook source
# MAGIC %md
# MAGIC Data reading
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

sales_df = spark.read.format("parquet")\
            .option('inferSchema',True)\
            .load("abfss://bronze@adlscarsales.dfs.core.windows.net/")

# COMMAND ----------

sales_df.display()

# COMMAND ----------

sales_df = sales_df.withColumn('Model_Category',split(col('Model_ID'),'-')[0])

# COMMAND ----------

sales_df.display()

# COMMAND ----------

sales_df = sales_df.withColumn('RevPerUnit',col('Revenue')/col('Units_Sold'))

# COMMAND ----------

# MAGIC %md
# MAGIC Checking null values

# COMMAND ----------

sales_df.select([count(when(col(c).isNull(), c)).alias(c) for c in sales_df.columns]).show()

# COMMAND ----------

sales_df.filter(col('DealerName').isNull()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Writing 

# COMMAND ----------

sales_df.where(col('BranchName') == col('DealerName')).display()

# COMMAND ----------

sales_df.write.format('parquet')\
                .mode('overwrite')\
                .option('path','abfss://silver@adlscarsales.dfs.core.windows.net/')\
                .save()

# COMMAND ----------



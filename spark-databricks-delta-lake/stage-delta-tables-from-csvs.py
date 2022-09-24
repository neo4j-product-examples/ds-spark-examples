# Databricks notebook source
# MAGIC %md
# MAGIC # Load HM CSV Files Into Delta Tables
# MAGIC 
# MAGIC This notebook stages the delta lake tables by loading them from csv files on the Databricks FIle System (DBFS).  If for any reason you need to re-upload the csv files follow the directions below:
# MAGIC 1. Download `articles.csv`, `customers.csv`, and `transaction_train.csv` from the Kaggle page: https://www.kaggle.com/competitions/h-and-m-personalized-fashion-recommendations/data
# MAGIC 2. create directory `/FileStore/hm-data/` in the DBFS and upload csvs to that directory. 
# MAGIC    1. `articles.csv` and `customers.csv` can be uploaded through either the UI or Databricks CLI
# MAGIC    2. `transaction_train.csv` must be uploaded through the Databricks CLI due to file size. The general command syntax is: `databricks fs cp transactions_train.csv dbfs:/FileStore/hm-data/transactions_train.csv`. Note it may take a while (like an hour).

# COMMAND ----------

from pyspark.sql.functions import expr

# COMMAND ----------

# MAGIC %md
# MAGIC ## Articles

# COMMAND ----------

df = spark.read.format("csv") \
  .option("inferSchema", "true") \
  .option("header", "true") \
  .load("/FileStore/hm-data/articles.csv")

table_name = "hmArticles"
df.write \
  .format("delta") \
  .saveAsTable(table_name)

display(spark.sql("SELECT * FROM " + table_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers

# COMMAND ----------

df = spark.read.format("csv") \
  .option("inferSchema", "true") \
  .option("header", "true") \
  .load("/FileStore/hm-data/customers.csv")

table_name = "hmCustomers"
df.write \
  .format("delta") \
  .saveAsTable(table_name)

display(spark.sql("SELECT * FROM " + table_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transactions

# COMMAND ----------

# Use the cli to upload csv...too big for UI upload, i.e. $databricks fs cp transactions_train.csv dbfs:/FileStore/hm-data/transactions_train.csv
df = spark.read.format("csv") \
  .option("inferSchema", "true") \
  .option("header", "true") \
  .load("/FileStore/hm-data/transactions_train.csv") \
  .withColumn("transaction_id", expr("uuid()")) # good practice to simulate with a unique id for each transaction (unique id absent from source data)

table_name = "hmTransactions"
df.write \
  .format("delta") \
  .saveAsTable(table_name)

display(spark.sql("SELECT * FROM " + table_name))

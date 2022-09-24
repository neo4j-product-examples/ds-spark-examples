# Databricks notebook source
# MAGIC %md
# MAGIC # ETL: Delta Lake -> Neo4j
# MAGIC __Data Source__: [Kaggle H&M Personalized Fashion Recommendations](https://www.kaggle.com/competitions/h-and-m-personalized-fashion-recommendations/data)
# MAGIC 
# MAGIC <img src="/files/hm-data/summary.png" alt="summary" width="1000"/>

# COMMAND ----------

NEO4J_URL = dbutils.secrets.get(scope = "product-demo", key = "aurads1URL")
NEO4J_USERNAME = "neo4j"
NEO4J_PASSWORD = dbutils.secrets.get(scope = "product-demo", key = "aurads1Password")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Customers

# COMMAND ----------

# read table
df = spark.table('hmCustomers')

#write to Neo4j
df.write.format("org.neo4j.spark.DataSource") \
  .mode("Overwrite") \
  .option("url", NEO4J_URL) \
  .option("authentication.basic.username", NEO4J_USERNAME) \
  .option("authentication.basic.password", NEO4J_PASSWORD) \
  .option("labels", "Customer") \
  .option("node.keys", "customer_id") \
  .option("schema.optimization.type", "NODE_CONSTRAINTS") \
  .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Articles

# COMMAND ----------

# read table
df = spark.table('hmArticles')

#write to Neo4j
df.write.format("org.neo4j.spark.DataSource") \
  .mode("Overwrite") \
  .option("url", NEO4J_URL) \
  .option("authentication.basic.username", NEO4J_USERNAME) \
  .option("authentication.basic.password", NEO4J_PASSWORD) \
  .option("labels", "Article") \
  .option("node.keys", "article_id") \
  .option("schema.optimization.type", "NODE_CONSTRAINTS") \
  .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Product & Product Relationships

# COMMAND ----------

# write products
df.select("product_code", "prod_name", "product_type_name", "product_type_no", "product_group_name").dropDuplicates(["product_code"]) \
  .write.format("org.neo4j.spark.DataSource") \
  .mode("Overwrite") \
  .option("url", NEO4J_URL) \
  .option("authentication.basic.username", NEO4J_USERNAME) \
  .option("authentication.basic.password", NEO4J_PASSWORD) \
  .option("labels", "Product") \
  .option("node.keys", "product_code") \
  .option("schema.optimization.type", "NODE_CONSTRAINTS") \
  .save()

# COMMAND ----------

# write product relationships
df.write.format("org.neo4j.spark.DataSource") \
  .mode("Append") \
  .option("url", NEO4J_URL) \
  .option("authentication.basic.username", NEO4J_USERNAME) \
  .option("authentication.basic.password", NEO4J_PASSWORD) \
  .option( "query", """
      MATCH(a:Article {article_id: event.article_id})
      MATCH(p:Product {product_code:event.product_code})
      MERGE(a)-[r:IS_VARIANT_OF]->(p)
      """) \
  .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Purchase Transactions

# COMMAND ----------

FILTER_TRANSACTIONS_BY_DATE = False
MINIMUM_DATE = '2020-08-11' #yyyy-mm-dd
USE_REL_MERGE = False

# COMMAND ----------

# read table
if FILTER_TRANSACTIONS_BY_DATE:
  df = spark.table('hmTransactions').where(f"t_dat >= date'{MINIMUM_DATE}'") #filtering to last 6 weeks of data for quick real-time demo
else:
  df = spark.table('hmTransactions')
  
#write to Neo4j
if USE_REL_MERGE:
  df.repartition(1).write.format("org.neo4j.spark.DataSource") \
    .mode("Append") \
    .option("url", NEO4J_URL) \
    .option("authentication.basic.username", NEO4J_USERNAME) \
    .option("authentication.basic.password", NEO4J_PASSWORD) \
    .option("batch.size", 50000) \
    .option("query", """
          MATCH(c:Customer {customer_id: event.customer_id})
          MATCH(a:Article {article_id: event.article_id})
          MERGE(c)-[r:PURCHASED {transaction_id: event.transaction_id}]->(a)
            ON CREATE SET r.transaction_date=event.t_dat, r.price=event.price
          """) \
    .save()
else:
  df.repartition(100, "article_id").write.format("org.neo4j.spark.DataSource") \
  .mode("Append") \
  .option("url", NEO4J_URL) \
  .option("authentication.basic.username", NEO4J_USERNAME) \
  .option("authentication.basic.password", NEO4J_PASSWORD) \
  .option("batch.size", 50000) \
  .option("query", """
        MATCH(c:Customer {customer_id: event.customer_id})
        MATCH(a:Article {article_id: event.article_id})
        CREATE(c)-[r:PURCHASED {transaction_id: event.transaction_id, transaction_date: event.t_dat, price: event.price}]->(a)
        """) \
  .save()

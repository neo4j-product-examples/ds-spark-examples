# Databricks notebook source
# MAGIC %md
# MAGIC # Load TPC-H From Snowflake into Neo4j

# COMMAND ----------

# Databricks notebook source
from neo4j_dwh_connector import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Default Source and Target Options

# COMMAND ----------

default_source_options={
  'sfURL': dbutils.secrets.get(scope = 'product-demo', key = 'snowflakeURL'),
  'sfUser': 'zach',
  'sfPassword': dbutils.secrets.get(scope = 'product-demo', key = 'snowflakePassword'),
  'sfDatabase': 'SNOWFLAKE_SAMPLE_DATA',
  'sfSchema': 'TPCH_SF1'
}

default_target_options={
  'url': dbutils.secrets.get(scope = 'product-demo', key = 'aurads4URL'),
  'authentication.basic.username': 'neo4j',
  'authentication.basic.password': dbutils.secrets.get(scope = 'product-demo', key = 'aurads4Password'),
  'schema.optimization.type': 'NODE_CONSTRAINTS' #ensures schema makes optimial node constraints
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Nodes

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Define Node Sources (Snowflake)

# COMMAND ----------

customer_property_source = Source(
    format="snowflake",
    options={**default_source_options, **{"dbtable":"CUSTOMER"}},
    columns=[
        Column(name="CAST(C_CUSTKEY AS LONG)", alias="customerKey"),
        Column(name="C_MKTSEGMENT", alias="marketSegment")
    ],
    printSchema=True
)

order_property_source = Source(
    format="snowflake",
    options={**default_source_options, **{"dbtable":"ORDERS"}},
    columns=[
      Column(name='CAST(O_ORDERKEY AS LONG)', alias='orderKey'),
      Column(name='O_ORDERSTATUS', alias='orderStatus'),
      Column(name='CAST(O_TOTALPRICE AS DOUBLE)', alias='totalPrice'),
      Column(name='O_ORDERDATE', alias='orderDate'),
      Column(name='O_ORDERPRIORITY', alias='orderPriority')
    ],
    printSchema=True
)

supplier_part_property_source = Source(
    format="snowflake",
    options={**default_source_options, **{"dbtable":"PARTSUPP"}},
    columns=[
      Column(name='CAST(PS_PARTKEY AS LONG)', alias='psPartKey'),
      Column(name='CAST(PS_SUPPKEY AS LONG)', alias='psSupplierKey'),
      Column(name='CAST(PS_SUPPLYCOST AS DOUBLE)', alias="supplyCost")
    ],
    printSchema=True
)

supplier_property_source = Source(
    format="snowflake",
    options={**default_source_options, **{"dbtable":"SUPPLIER"}},
    columns=[
        Column(name="CAST(S_SUPPKEY AS LONG)", alias="supplierKey")
    ],
    printSchema=True
)

part_property_source = Source(
    format="snowflake",
    options={**default_source_options, **{"dbtable":"PART"}},
    columns=[
      Column(name='CAST(P_PARTKEY AS LONG)', alias="partKey"),
      Column(name='P_NAME', alias="partName"),
      Column(name='P_MFGR', alias="manufacturer"),
      Column(name='P_BRAND', alias="type"),
      Column(name='P_TYPE', alias="size"),
      Column(name='P_CONTAINER', alias="container"),
      Column(name='CAST(P_RETAILPRICE AS DOUBLE)', alias="retailPrice"),
    ],
    printSchema=True
)

nation_property_source = Source(
    format="snowflake",
    options={**default_source_options, **{"dbtable":"NATION"}},
    columns=[
        Column(name="CAST(N_NATIONKEY AS LONG)", alias="nationKey"),
        Column(name="N_NAME", alias="nationName")
    ],
    printSchema=True
)

region_property_source = Source(
    format="snowflake",
    options={**default_source_options, **{"dbtable":"REGION"}},
    columns=[
        Column(name="CAST(R_REGIONKEY AS LONG)", alias="regionKey"),
        Column(name="R_NAME", alias="regionName")
    ],
    printSchema=True
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Node Targets (Neo4j)

# COMMAND ----------

customer_target =  Target(
    format="org.neo4j.spark.DataSource", 
    options={**default_target_options, **{"labels": "Customer", "node.keys": "customerKey"}},
    mode="Overwrite"
)

order_target =  Target(
    format="org.neo4j.spark.DataSource", 
    options={**default_target_options, **{"labels": "Order", "node.keys": "orderKey"}},
    mode="Overwrite"
)

supplier_part_target =  Target(
    format="org.neo4j.spark.DataSource", 
    options={**default_target_options, **{ "labels": "SupplierPart", "node.keys": "psPartKey,psSupplierKey"}},
    mode="Overwrite"
)

part_target =  Target(
    format="org.neo4j.spark.DataSource", 
    options={**default_target_options, **{"labels": "Part", "node.keys": "partKey"}},
    mode="Overwrite"
)

supplier_target =  Target(
    format="org.neo4j.spark.DataSource", 
    options={**default_target_options, **{"labels": "Supplier", "node.keys": "supplierKey"}},
    mode="Overwrite"
)

nation_target =  Target(
    format="org.neo4j.spark.DataSource", 
    options={**default_target_options, **{"labels": "Nation", "node.keys": "nationKey"}},
    mode="Overwrite"
)

region_target =  Target(
    format="org.neo4j.spark.DataSource", 
    options={**default_target_options, **{"labels": "Region", "node.keys": "regionKey"}},
    mode="Overwrite"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Node Loading Jobs

# COMMAND ----------

conn = Neo4jDWHConnector(spark, JobConfig(name="load-customer", conf={}, hadoopConfiguration={}, source=customer_property_source, target=customer_target))
conn.run()

# COMMAND ----------

conn = Neo4jDWHConnector(spark, JobConfig(name="load-order", conf={}, hadoopConfiguration={}, source=order_property_source, target=order_target))
conn.run()

# COMMAND ----------

conn = Neo4jDWHConnector(spark, JobConfig(name="load-supplier-part", conf={}, hadoopConfiguration={}, source=supplier_part_property_source, target=supplier_part_target))
conn.run()

# COMMAND ----------

conn = Neo4jDWHConnector(spark, JobConfig(name="load-part", conf={}, hadoopConfiguration={}, source=part_property_source, target=part_target))
conn.run()

# COMMAND ----------

conn = Neo4jDWHConnector(spark, JobConfig(name="load-supplier", conf={}, hadoopConfiguration={}, source=supplier_property_source, target=supplier_target))
conn.run()

# COMMAND ----------

conn = Neo4jDWHConnector(spark, JobConfig(name="load-nation", conf={}, hadoopConfiguration={}, source=nation_property_source, target=nation_target))
conn.run()

# COMMAND ----------

conn = Neo4jDWHConnector(spark, JobConfig(name="load-region", conf={}, hadoopConfiguration={}, source=region_property_source, target=region_target))
conn.run()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Relationships

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Relationship Sources (Snowflake)

# COMMAND ----------

ordered_source = Source(
    format="snowflake",
    options={**default_source_options, **{"dbtable":"ORDERS"}},
    columns=[
        Column(name="CAST(O_CUSTKEY AS LONG)", alias="customerKey"),
        Column(name='CAST(O_ORDERKEY AS LONG)', alias='orderKey')
    ],
    printSchema=True,
    partition = Partition(number=1, by="customerKey")
)


has_line_item_source = Source(
    format="snowflake",
    options={**default_source_options, **{"dbtable":"LINEITEM"}},
    columns=[
      Column(name='CAST(L_ORDERKEY AS LONG)', alias="orderKey"),
      Column(name='CAST(L_LINENUMBER AS LONG)', alias="lineNumber"),
      Column(name='CAST(L_PARTKEY AS LONG)', alias="psPartKey"),
      Column(name='CAST(L_SUPPKEY AS LONG)', alias="psSupplierKey"),

      Column(name='CAST(L_QUANTITY AS DOUBLE)', alias="quantity"),
      Column(name='CAST(L_EXTENDEDPRICE AS DOUBLE)', alias="extendedPrice"),
      Column(name='CAST(L_DISCOUNT AS DOUBLE)', alias="discount"),
      Column(name='CAST(L_TAX AS DOUBLE)', alias="tax"),
      Column(name='L_RETURNFLAG', alias="returnFlag"),
      Column(name='L_LINESTATUS', alias="lineStatus")
    ],
    printSchema=True,
    partition = Partition(number=1, by="orderKey")
)

is_part_from_supp_source = Source(
    format="snowflake",
    options={**default_source_options, **{"dbtable":"PARTSUPP"}},
    columns=[
      Column(name='CAST(PS_PARTKEY AS LONG)', alias='partKey'),
      Column(name='CAST(PS_SUPPKEY AS LONG)', alias='supplierKey'),
      Column(name='CAST(PS_PARTKEY AS LONG)', alias='psPartKey'),
      Column(name='CAST(PS_SUPPKEY AS LONG)', alias='psSupplierKey')
    ],
    printSchema=True,
    partition = Partition(number=1, by="partKey")
)

customer_from_source = Source(
    format="snowflake",
    options={**default_source_options, **{"dbtable":"CUSTOMER"}},
    columns=[
      Column(name="CAST(C_CUSTKEY AS LONG)", alias="customerKey"),
      Column(name="CAST(C_NATIONKEY AS LONG)", alias="nationKey")
    ],
    printSchema=True,
    partition = Partition(number=1, by="nationKey")
)


supplier_from_source = Source(
    format="snowflake",
    options={**default_source_options, **{"dbtable":"SUPPLIER"}},
    columns=[
      Column(name="CAST(S_SUPPKEY AS LONG)", alias="supplierKey"),
      Column(name="CAST(S_NATIONKEY AS LONG)", alias="nationKey")
    ],
    printSchema=True,
    partition = Partition(number=1, by="nationKey")
)

nation_located_in_source = Source(
    format="snowflake",
    options={**default_source_options, **{"dbtable":"NATION"}},
    columns=[
        Column(name="CAST(N_NATIONKEY AS LONG)", alias="nationKey"),
        Column(name="CAST(N_REGIONKEY AS LONG)", alias="regionKey")
    ],
    printSchema=True,
  partition = Partition(number=1, by="regionKey")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Relationship targets (Neo4j)

# COMMAND ----------

ordered_target = Target(
    format="org.neo4j.spark.DataSource", 
    options={**default_target_options, **{
     "relationship" : "ORDERED",
     "relationship.save.strategy" : "keys",
     "relationship.source.save.mode" : "Overwrite",
     "relationship.source.labels" : "Customer",
     "relationship.source.node.keys" : "customerKey",
     "relationship.target.save.mode" : "Overwrite",
     "relationship.target.labels" : "Order",
     "relationship.target.node.keys" : "orderKey"
    }},
    mode="Overwrite"
)

has_line_item_target = Target(
    format="org.neo4j.spark.DataSource", 
    options={**default_target_options, **{
     "relationship" : "HAS_LINE_ITEM",
     "relationship.save.strategy" : "keys",
     "relationship.source.save.mode" : "Overwrite",
     "relationship.source.labels" : "Order",
     "relationship.source.node.keys" : "orderKey",
     "relationship.target.save.mode" : "Overwrite",
     "relationship.target.labels" : "SupplierPart",
     "relationship.target.node.keys" : "psPartKey,psSupplierKey"
    }},
    mode="Overwrite"
)

is_part_target = Target(
    format="org.neo4j.spark.DataSource", 
    options={**default_target_options, **{
     "relationship" : "IS_PART",
     "relationship.save.strategy" : "keys",
     "relationship.source.save.mode" : "Overwrite",
     "relationship.source.labels" : "SupplierPart",
     "relationship.source.node.keys" : "psPartKey,psSupplierKey",
     "relationship.target.save.mode" : "Overwrite",
     "relationship.target.labels" : "Part",
     "relationship.target.node.keys" : "partKey"
    }},
    mode="Overwrite"
)

from_supplier_target = Target(
    format="org.neo4j.spark.DataSource", 
    options={**default_target_options, **{
     "relationship" : "FROM_SUPPLIER",
     "relationship.save.strategy" : "keys",
     "relationship.source.save.mode" : "Overwrite",
     "relationship.source.labels" : "SupplierPart",
     "relationship.source.node.keys" : "psPartKey,psSupplierKey",
     "relationship.target.save.mode" : "Overwrite",
     "relationship.target.labels" : "Supplier",
     "relationship.target.node.keys" : "supplierKey"
    }},
    mode="Overwrite"
)

customer_from_target = Target(
    format="org.neo4j.spark.DataSource", 
    options={**default_target_options, **{
     "relationship" : "FROM",
     "relationship.save.strategy" : "keys",
     "relationship.source.save.mode" : "Overwrite",
     "relationship.source.labels" : "Customer",
     "relationship.source.node.keys" : "customerKey",
     "relationship.target.save.mode" : "Overwrite",
     "relationship.target.labels" : "Nation",
     "relationship.target.node.keys" : "nationKey"
    }},
    mode="Overwrite"
)


supplier_from_target = Target(
    format="org.neo4j.spark.DataSource", 
    options={**default_target_options, **{
     "relationship" : "FROM",
     "relationship.save.strategy" : "keys",
     "relationship.source.save.mode" : "Overwrite",
     "relationship.source.labels" : "Supplier",
     "relationship.source.node.keys" : "supplierKey",
     "relationship.target.save.mode" : "Overwrite",
     "relationship.target.labels" : "Nation",
     "relationship.target.node.keys" : "nationKey"
    }},
    mode="Overwrite"
)


nation_located_in_target = Target(
    format="org.neo4j.spark.DataSource", 
    options={**default_target_options, **{
     "relationship" : "LOCATED_IN",
     "relationship.save.strategy" : "keys",
     "relationship.source.save.mode" : "Overwrite",
     "relationship.source.labels" : "Nation",
     "relationship.source.node.keys" : "nationKey",
     "relationship.target.save.mode" : "Overwrite",
     "relationship.target.labels" : "Region",
     "relationship.target.node.keys" : "regionKey"
    }},
    mode="Overwrite"
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Relationship Loading Jobs

# COMMAND ----------

conn = Neo4jDWHConnector(spark, JobConfig(name="load-ordered-rels", conf={}, hadoopConfiguration={}, source=ordered_source, target=ordered_target))
conn.run()

# COMMAND ----------

conn = Neo4jDWHConnector(spark, JobConfig(name="load-lineitem-rels", conf={}, hadoopConfiguration={}, source=has_line_item_source, target=has_line_item_target))
conn.run()

# COMMAND ----------

conn = Neo4jDWHConnector(spark, JobConfig(name="load-is-part-rels", conf={}, hadoopConfiguration={}, source=is_part_from_supp_source, target=is_part_target))
conn.run()

# COMMAND ----------

conn = Neo4jDWHConnector(spark, JobConfig(name="load-from-supplier-rels", conf={}, hadoopConfiguration={}, source=is_part_from_supp_source, target=from_supplier_target))
conn.run()

# COMMAND ----------

conn = Neo4jDWHConnector(spark, JobConfig(name="load-customer-from-rels", conf={}, hadoopConfiguration={}, source=customer_from_source, target=customer_from_target))
conn.run()

# COMMAND ----------

conn = Neo4jDWHConnector(spark, JobConfig(name="load-supplier-from-rels", conf={}, hadoopConfiguration={}, source=supplier_from_source, target=supplier_from_target))
conn.run()

# COMMAND ----------

conn = Neo4jDWHConnector(spark, JobConfig(name="load-located-in-rels", conf={}, hadoopConfiguration={}, source=nation_located_in_source, target=nation_located_in_target))
conn.run()

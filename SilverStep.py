# Databricks notebook source
# MAGIC %sql
# MAGIC select * from workspace.default.raw_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC select customer_id, count(*)
# MAGIC from workspace.default.raw_customers
# MAGIC group by customer_id
# MAGIC having count(*) >1

# COMMAND ----------

# MAGIC %sql
# MAGIC select order_id, count(*) from workspace.default.raw_orders
# MAGIC group by order_id
# MAGIC having count(*) >1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.default.raw_orders
# MAGIC where order_id = 'O0000675'

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table workspace.default.silver_orders as 
# MAGIC select * from workspace.default.raw_orders
# MAGIC qualify row_number() over(partition by order_id order by ingest_time desc) = 1
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct order_id) cnt, count(*) from workspace.default.raw_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct order_id) cnt, count(*) from workspace.default.silver_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct payment_id) cnt, count(*) from workspace.default.raw_payments

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view raw_orders_new_batch_vw as
# MAGIC select * from workspace.default.raw_orders_new_batch
# MAGIC qualify row_number() over(partition by order_id order by ingest_time desc) = 1
# MAGIC

# COMMAND ----------

spark.sql("""
MERGE INTO workspace.default.silver_orders AS target
USING raw_orders_new_batch_vw AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct order_id) cnt, count(*) from workspace.default.silver_orders

# COMMAND ----------

display(spark.sql("""
SELECT COUNT(*) AS num_changed_records
FROM (
  SELECT *
  FROM workspace.default.silver_orders VERSION AS OF 1
  EXCEPT
  SELECT *
  FROM workspace.default.silver_orders VERSION AS OF 0
  UNION ALL
  SELECT *
  FROM workspace.default.silver_orders VERSION AS OF 0
  EXCEPT
  SELECT *
  FROM workspace.default.silver_orders VERSION AS OF 1
)
"""))

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT *
# MAGIC   FROM workspace.default.silver_orders VERSION AS OF 1
# MAGIC   EXCEPT
# MAGIC   SELECT *
# MAGIC   FROM workspace.default.silver_orders VERSION AS OF 0
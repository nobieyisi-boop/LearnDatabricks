# Databricks notebook source
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
# MAGIC create or replace table gold_payments_dataset as 
# MAGIC select p.*, o.customer_id, o.order_timestamp,o.order_amount, o.status, c.signup_timestamp, c.country, c.plan
# MAGIC from raw_payments p
# MAGIC left join silver_orders o on p.order_id = o.order_id
# MAGIC left join raw_customers c on o.customer_id = c.customer_id
# MAGIC
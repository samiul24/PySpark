# Databricks notebook source
# MAGIC %md
# MAGIC &copy; This is Databricks course
# MAGIC

# COMMAND ----------

msg='I am learning DB'
print(msg)

# COMMAND ----------

# MAGIC %sql
# MAGIC select 'DB'

# COMMAND ----------

# MAGIC %scala
# MAGIC var msg = "Scala"
# MAGIC print(msg)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/

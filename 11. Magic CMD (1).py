# Databricks notebook source
# MAGIC %md
# MAGIC &copy; This is Databricks course
# MAGIC

# COMMAND ----------

msg='I am learning DB'
print(msg)

# COMMAND ----------

#All magic cmd
%lsmagic

# COMMAND ----------

#To see the details of a cmd
%env?

# COMMAND ----------

# MAGIC %pwd?

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

#To see the file list
%fs
ls /databricks-datasets/

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /databricks-datasets/airlines/

# COMMAND ----------

#Shell CMD to see the including data
%sh
tail /databricks-datasets/airlines/part-00000

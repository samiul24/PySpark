# Databricks notebook source
dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.help("cp")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /databricks-datasets/

# COMMAND ----------

#Difference btw ls and dbutils
dbutils.fs.ls("/databricks-datasets")

# COMMAND ----------

for folder_name in dbutils.fs.ls("/databricks-datasets"):
    print(folder_name.path)

# COMMAND ----------

#notebook
dbutils.notebook.help()

# COMMAND ----------

#call a notebook
dbutils.notebook.run('./13_child_notebook',20)

# Databricks notebook source
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,configuration
spark.conf.set("fs.azure.account.key.azurefinaladls.dfs.core.windows.net","Nyu6vXt7poXvmzgFWNnqPR/731nblT0m9N9ApOPzsCCvAYHh4Jmt4hn/acOkwi58WW47yv7Eay6K1HbfOzQ9aA==")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")

# COMMAND ----------

#spark.conf.set("fs.azure.account.key.<storage-account-name>.dfs.core.windows.net",dbutils.secrets.get(scope="<scope-name>",key="<storage-account-access-key-name>"))

# COMMAND ----------

# DBTITLE 1,read src data 
src_path = 'abfss://input@azurefinaladls.dfs.core.windows.net/students.csv'
print(src_path)

src_df=spark.read.csv(src_path, header=True)

# COMMAND ----------

src_df.printSchema()

# COMMAND ----------

src_df.show()

# COMMAND ----------

display(src_df)

# COMMAND ----------

delta_path = 'abfss://output@azurefinaladls.dfs.core.windows.net/delta/student2/'
merge_condition = "s.sid=t.sid"

if DeltaTable.isDeltaTable(spark, delta_path): #False
  print('merge load')
  target_df = DeltaTable.forPath(spark, delta_path)
  target_df.alias("t").merge(src_df.alias("s"),merge_condition).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
  print("Initial load")
  src_df.write.format("delta").option("mergeSchema","true").save(delta_path)

# COMMAND ----------

# DBTITLE 1,create a delta table
# MAGIC %sql
# MAGIC drop table if exists student_table_inc1;
# MAGIC create table if not exists student_table_inc1 using delta location 'abfss://output@azurefinaladls.dfs.core.windows.net/delta/student2/';
# MAGIC refresh table student_table_inc1;
# MAGIC 
# MAGIC drop view if exists student_view2;
# MAGIC create view student_view2 as select * from student_table_inc1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from student_view2;

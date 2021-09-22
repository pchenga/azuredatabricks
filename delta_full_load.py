# Databricks notebook source
# DBTITLE 1,configuration
spark.conf.set("fs.azure.account.key.azurefinaladls.dfs.core.windows.net","Nyu6vXt7poXvmzgFWNnqPR/731nblT0m9N9ApOPzsCCvAYHh4Jmt4hn/acOkwi58WW47yv7Eay6K1HbfOzQ9aA==")

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

# DBTITLE 1,write to delta
delta_path = 'abfss://output@azurefinaladls.dfs.core.windows.net/delta/student/'

src_df.write.mode("overwrite").format("delta").option("overwriteSchema","true").save(delta_path)


# COMMAND ----------

# DBTITLE 1,create a delta table
# MAGIC %sql
# MAGIC drop table if exists student_table;
# MAGIC create table if not exists student_table using delta location 'abfss://output@azurefinaladls.dfs.core.windows.net/delta/student/';
# MAGIC refresh table student_table;
# MAGIC 
# MAGIC drop view if exists student_view;
# MAGIC create view student_view as select * from student_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from student_view;

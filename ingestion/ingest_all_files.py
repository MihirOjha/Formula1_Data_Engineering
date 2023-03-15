# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_circuits_file",0,{"p_data_source":"ErgastAPI","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_constructor_file",0,{"p_data_source":"ErgastAPI","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_drivers_file",0,{"p_data_source":"ErgastAPI","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_lap_times_file",0,{"p_data_source":"ErgastAPI","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_pitstops_file",0,{"p_data_source":"ErgastAPI","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_qualifying_file",0,{"p_data_source":"ErgastAPI","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_races_file",0,{"p_data_source":"ErgastAPI","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_results_file",0,{"p_data_source":"ErgastAPI","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------


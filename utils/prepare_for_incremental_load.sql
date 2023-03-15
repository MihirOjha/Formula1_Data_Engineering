-- Databricks notebook source
DROP DATABASE IF EXISTS processed CASCADE

-- COMMAND ----------

DROP DATABASE IF EXISTS presentation CASCADE

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS processed
LOCATION "/mnt/bigdatastorage10/processed";

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS presentation
LOCATION "/mnt/bigdatastorage10/presentation";

-- COMMAND ----------


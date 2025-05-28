# Databricks notebook source
# MAGIC %md
# MAGIC ## Create Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG cars_catalog;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create schemas for Silver and Gold layer.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA cars_catalog.silver;
# MAGIC CREATE SCHEMA cars_catalog.gold;
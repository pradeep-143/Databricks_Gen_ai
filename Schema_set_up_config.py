# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Notebook Overview
# MAGIC %md
# MAGIC # Resume Use Case - Schema Setup Configuration
# MAGIC
# MAGIC This notebook sets up the foundational data architecture for the resume use case project:
# MAGIC
# MAGIC **Catalog Structure:**
# MAGIC * Creates the `resume_use_case` Unity Catalog
# MAGIC * Establishes a medallion architecture with four schemas:
# MAGIC   * **staging** - Raw data landing zone
# MAGIC   * **bronze** - Raw ingested data
# MAGIC   * **silver** - Cleaned and validated data
# MAGIC   * **gold** - Business-level aggregates and analytics-ready data
# MAGIC
# MAGIC **Storage:**
# MAGIC * Creates a Unity Catalog volume `source_files` in the staging schema for storing raw resume files
# MAGIC
# MAGIC Run all cells in sequence to initialize the complete data infrastructure.

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists resume_use_case;

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog resume_use_case;

# COMMAND ----------

# DBTITLE 1,Create staging,bronze, silver, gold schemas
# Create bronze, silver, and gold schemas
schemas = ['staging','bronze', 'silver', 'gold']

for schema in schemas:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    print(f"Schema '{schema}' created successfully")

# COMMAND ----------

# MAGIC %sql
# MAGIC create volume if not exists resume_use_case.staging.source_files;

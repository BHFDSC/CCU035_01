# Databricks notebook source
# MAGIC %md #Primary care: CVD outcomes (work package 2.5)  
# MAGIC 
# MAGIC **Description** This notebooks make a table `CCU035_cvd_outcomes_gdppr` which contains all observations with the relevant observations containing outcomes clinical codes from the GDPPR dataset, where events happened from  **covid19_confirmed_date**. Phenotypes used are listed below:
# MAGIC 
# MAGIC 
# MAGIC **Project(s)** CCU035_01
# MAGIC  
# MAGIC **Author(s)** Spencer Keene adapted from Rachel Denholm's notebooks. Updated by Hannah Whittaker for project ccu03_01
# MAGIC  
# MAGIC **Reviewer(s)** 
# MAGIC  
# MAGIC **Date last updated** 2022-03-24
# MAGIC  
# MAGIC **Date last reviewed** 
# MAGIC  
# MAGIC **Date last run** 
# MAGIC  
# MAGIC **Data input** GDPPR
# MAGIC 
# MAGIC **Data output** table: `CCU035_cvd_outcomes_gdppr`
# MAGIC 
# MAGIC **Software and versions** SQL
# MAGIC  
# MAGIC **Packages and versions** 'Not applicable'

# COMMAND ----------

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window
import databricks.koalas as ks
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re
import datetime

# COMMAND ----------

# MAGIC %run "/Workspaces/dars_nic_391419_j3w9t_collab/CCU035_01/CCU035_01-functions/covariate_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #####Infection parameters

# COMMAND ----------

# MAGIC %py
# MAGIC 
# MAGIC #Dataset parameters
# MAGIC gdppr = 'dars_nic_391419_j3w9t_collab.ccu035_01_gdppr_dars_nic_391419_j3w9t'
# MAGIC index_dates='dars_nic_391419_j3w9t_collab.ccu035_01_start_and_end_date'
# MAGIC project_prefix = 'ccu035_01_'
# MAGIC collab_database_name = 'dars_nic_391419_j3w9t_collab'
# MAGIC 
# MAGIC #Date parameters
# MAGIC #index_date = '2020-01-01'
# MAGIC #end_date = '2020-12-07'
# MAGIC 
# MAGIC #Final table name
# MAGIC final_table ='ccu035_01_inf_outcomes_gdppr_final'   

# COMMAND ----------

# MAGIC %py
# MAGIC #Creating global temp for primary care data
# MAGIC spark.sql(F"""create or replace global temp view CCU035_01_gdppr_infection
# MAGIC as select *
# MAGIC from {gdppr}""") 

# COMMAND ----------

# MAGIC %sql
# MAGIC ----Relevant outcomes codes
# MAGIC 
# MAGIC create or replace global temp view CCU035_01_infection_gdpproutcomes_map as
# MAGIC 
# MAGIC SELECT name, terminology, code, term, code_type, RecordDate
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu035_01_codelist
# MAGIC WHERE (name = 'AMI' OR name = 'HF' OR name = 'angina' OR name = 'stroke_TIA' OR name = 'stroke_isch' OR name = 'unstable_angina')
# MAGIC       AND terminology = 'SNOMED' --AND code_type=1

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Available outcomes in the GDPPR dataset
# MAGIC 
# MAGIC select name
# MAGIC from global_temp.CCU035_01_infection_gdpproutcomes_map
# MAGIC group by name

# COMMAND ----------

index_dates=spark.table (f'dars_nic_391419_j3w9t_collab.ccu035_01_start_and_end_date')

# COMMAND ----------

#merge in end date frin D11 worksapce -HW 
gdppr = spark.table(f'global_temp.{project_prefix}gdppr_infection')\
  .withColumnRenamed('person_id_deid', 'NHS_NUMBER_DEID')\
  .join(index_dates, on='NHS_NUMBER_DEID', how='inner')
gdppr.createOrReplaceGlobalTempView(f'{project_prefix}gdppr_infection')


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE dars_nic_391419_j3w9t_collab.CCU035_01_infection_cvd_outcomes_gdppr AS
# MAGIC 
# MAGIC with cte_gdppr as (
# MAGIC SELECT NHS_NUMBER_DEID, DATE, CODE as snomed
# MAGIC FROM global_temp.CCU035_01_gdppr_infection
# MAGIC WHERE DATE > covid19_confirmed_date AND DATE <= end_date
# MAGIC )
# MAGIC  
# MAGIC select *
# MAGIC from cte_gdppr t1
# MAGIC inner join global_temp.CCU035_01_infection_gdpproutcomes_map t2 on (t1.snomed = t2.code)

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE dars_nic_391419_j3w9t_collab.CCU035_01_infection_cvd_outcomes_gdppr OWNER TO dars_nic_391419_j3w9t_collab 

# COMMAND ----------

# MAGIC %sql
# MAGIC ---Number of patients with specific conditions
# MAGIC 
# MAGIC SELECT count(Distinct NHS_NUMBER_DEID) as count, name
# MAGIC FROM dars_nic_391419_j3w9t_collab.CCU035_01_infection_cvd_outcomes_gdppr
# MAGIC group by name
# MAGIC order by count desc 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(Distinct NHS_NUMBER_DEID) as count
# MAGIC FROM dars_nic_391419_j3w9t_collab.CCU035_01_infection_cvd_outcomes_gdppr

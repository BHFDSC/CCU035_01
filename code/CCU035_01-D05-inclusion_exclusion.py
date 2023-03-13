# Databricks notebook source
# MAGIC %md # CCU035_01-D04-inclusion_exclusion
# MAGIC  
# MAGIC **Description** This notebook runs through the inclusion/exclusion criteria for the skinny cohort after QA.
# MAGIC 
# MAGIC **Author(s)** Updated by Rochelle Knight for CCU002_01 from notebook by Jenny Cooper and Samantha Ip CCU002_02. Updated for CCU035_01 by Hannah Whittaker.
# MAGIC 
# MAGIC **Reviewer(s)** 
# MAGIC 
# MAGIC **Date last updated** 17-02-2022
# MAGIC 
# MAGIC **Date last reviewed**
# MAGIC 
# MAGIC **Data last run** 17-02-2022
# MAGIC 
# MAGIC **Data input** `dars_nic_391419_j3w9t_collab.ccu035_01_inf_conflictingpatients`
# MAGIC 
# MAGIC `dars_nic_391419_j3w9t_collab.ccu035_01_sgss_dars_nic_391419_j3w9t`
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Data output** `dars_nic_391419_j3w9t_collab.ccu035_01_inf_included_patients`
# MAGIC 
# MAGIC **Software and versions** SQL, Python
# MAGIC 
# MAGIC **Packages and versions** Not applicable

# COMMAND ----------

# MAGIC %run Workspaces/dars_nic_391419_j3w9t_collab/CCU035_01/CCU035_01-functions/wrang000_functions

# COMMAND ----------

# MAGIC %run "/Workspaces/dars_nic_391419_j3w9t_collab/CCU035_01/CCU035_01-D15-master_notebook_parameters"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Set up

# COMMAND ----------

#%sql
#--Run this only if updating tables

#--DROP VIEW IF EXISTS global_temp.patientinclusion;
#--DROP VIEW IF EXISTS global_temp.practicesover1;
#--DROP VIEW IF EXISTS global_temp.patients_died;
#--DROP VIEW IF EXISTS global_temp.positive_test;
#--DROP VIEW IF EXISTS global_temp.nhs_icd_exclude;
#--DROP VIEW IF EXISTS global_temp.nhs_snomed_exclude;


# COMMAND ----------

# MAGIC %md
# MAGIC ### Infection parameters

# COMMAND ----------

#Dataset Parameters 
skinny_data = 'inf_skinny_patient'
sgss_data = 'sgss_dars_nic_391419_j3w9t'

#Other data inputs
conflicting_quality_assurance = 'inf_skinny_conflicting_patients'

#Date parameters
index_date = '2020-01-01'

#Final table name
collab_database_name = 'dars_nic_391419_j3w9t_collab'
project_prefix = 'ccu035_01_'
incl_excl_table_name = 'inf_included_patients' 

# COMMAND ----------

from pyspark.sql.functions import countDistinct, year, dayofmonth, from_unixtime, month, unix_timestamp, to_timestamp, date_format, col, datediff, to_date, lit, months_between
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime
from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql import Window
import io
from functools import reduce
from pyspark.sql.types import StringType

# COMMAND ----------

# MAGIC %md
# MAGIC ### Anti-join QA-excluded IDs to skinny table -- gives skinny_withQA

# COMMAND ----------

#antijoin QA table to skinny table to remove conflicting patients identified from QA
spark.sql(f"""
create or replace global temp view {project_prefix}skinny_withQA as

SELECT t1.*
FROM {collab_database_name}.{project_prefix}{skinny_data} t1
LEFT JOIN {collab_database_name}.{project_prefix}{conflicting_quality_assurance} t2 ---those who didnt meet QA criteria, from the previous notebook
ON t1.nhs_number_deid = t2.nhs_number_deid
WHERE t2.nhs_number_deid IS NULL """)


# COMMAND ----------

# DBTITLE 1,Creating Temporary Tables for the inclusion and exclusion criteria
# MAGIC %py
# MAGIC #People to include:
# MAGIC #Known sex and age 18 and over -- people who have not died before 1st Jan 2020 and who have SEX==1/2 and who are over 18
# MAGIC spark.sql(F"""create or replace global temp view {project_prefix}patientinclusion AS 
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM global_temp.{project_prefix}skinny_withQA 
# MAGIC WHERE nhs_number_deid is not null 
# MAGIC AND AGE_AT_COHORT_START >=18 
# MAGIC AND (SEX =1 OR SEX=2)
# MAGIC AND ((DATE_OF_DEATH >= '{index_date}') or (DATE_OF_DEATH is null)) """)

# COMMAND ----------

# MAGIC %py
# MAGIC # Excluding those with a positive test prior to 1st Jan 2020 for vaccine work
# MAGIC spark.sql(F"""create or replace global temp view {project_prefix}positive_test_pre_2020 AS   
# MAGIC 
# MAGIC SELECT distinct(PERSON_ID_DEID) as nhs_number_deid --same nhs identifer name needed to union
# MAGIC FROM {collab_database_name}.{project_prefix}{sgss_data}
# MAGIC WHERE Lab_Report_date <'{index_date}' AND PERSON_ID_DEID is not null""")

# COMMAND ----------

# MAGIC %py
# MAGIC #anti-join inclusion population with exclusion NHS numbers
# MAGIC spark.sql(f"""
# MAGIC create or replace global temp view {project_prefix}{incl_excl_table_name} AS   
# MAGIC 
# MAGIC select 
# MAGIC NHS_NUMBER_DEID, SEX, CATEGORISED_ETHNICITY, ETHNIC, DATE_OF_BIRTH, DATE_OF_DEATH, AGE_AT_COHORT_START  --keep here if want to specify specific variables, otherwise not needed.
# MAGIC FROM
# MAGIC (
# MAGIC SELECT t1.*
# MAGIC FROM global_temp.{project_prefix}patientinclusion t1
# MAGIC LEFT JOIN  
# MAGIC 
# MAGIC (
# MAGIC SELECT * FROM global_temp.{project_prefix}positive_test_pre_2020 --positive COVID test
# MAGIC ) t2 
# MAGIC ON t1.nhs_number_deid = t2.nhs_number_deid
# MAGIC 
# MAGIC WHERE t2.nhs_number_deid IS NULL) """)
# MAGIC 
# MAGIC #Gives a list of nhs numbers to include in the study

# COMMAND ----------

# MAGIC %py
# MAGIC 
# MAGIC spark.sql(F"""DROP TABLE IF EXISTS {collab_database_name}.{project_prefix}{incl_excl_table_name}""")

# COMMAND ----------

create_table(project_prefix + incl_excl_table_name, select_sql_script=f"SELECT * FROM global_temp.{project_prefix}{incl_excl_table_name}") 

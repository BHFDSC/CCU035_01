# Databricks notebook source
# MAGIC %md # CCU035_01-D16-Vaccine_analysis_dates
# MAGIC  
# MAGIC **Description** Collate vaccine dates in table to import into STATA and continue data management, Already have date of death in STATA tables. Will create last collection date (30-11-2021) in STATA for ease. 
# MAGIC  
# MAGIC **Author(s)** Hannah Whittaker
# MAGIC 
# MAGIC **Project(s)** CCU035_01
# MAGIC 
# MAGIC **Reviewer(s)** 
# MAGIC  
# MAGIC **Date last updated** 25-08-2022
# MAGIC  
# MAGIC **Date last reviewed** UNREVIEWED
# MAGIC  
# MAGIC **Date last run** 
# MAGIC  
# MAGIC **Data input** 
# MAGIC 
# MAGIC **Data output** vaccine_analysis_dates
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Software and versions** SQL, Python
# MAGIC  
# MAGIC **Packages and versions** Not applicable

# COMMAND ----------

# MAGIC %md ## Data set up

# COMMAND ----------

# MAGIC %run "/Workspaces/dars_nic_391419_j3w9t_collab/CCU035_01/CCU035_01-functions/covariate_functions"

# COMMAND ----------

# MAGIC %run "/Workspaces/dars_nic_391419_j3w9t_collab/SHDS/common/functions"

# COMMAND ----------

from pyspark.sql.functions import countDistinct, year, dayofmonth, from_unixtime, month, unix_timestamp, to_timestamp, date_format, col, datediff, to_date, lit, months_between
import pandas as pd
#import numpy as np
import numpy as int
import seaborn as sns
import matplotlib.pyplot as plt
import datetime
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql import Window
import io
from functools import reduce
from pyspark.sql.types import StringType
import databricks.koalas as ks

# COMMAND ----------

#Dataset parameters
project_prefix = 'ccu035_01_'
collab_database_name = 'dars_nic_391419_j3w9t_collab'
vaccine_codelist = 'vaccine_codelist'
vaccine_data = 'vaccine_status_dars_nic_391419_j3w9t'
skinny_QA_inclusion_table = 'inf_included_patients' 

#Final table name
vaccine_dates_table = 'vaccine_analysis_dates'

# COMMAND ----------

# MAGIC %md ## Merge Tables with covid index date

# COMMAND ----------

vaccine_data = spark.table(f"dars_nic_391419_j3w9t_collab.ccu035_01vaccine_all")

covid_index_date = spark.table(f'{collab_database_name}.{project_prefix}start_and_end_date')\
  .withColumnRenamed('person_id_deid', 'NHS_NUMBER_DEID')

vaccine = vaccine_data\
  .join(covid_index_date, on='NHS_NUMBER_DEID', how='inner')\
  .select('NHS_NUMBER_DEID', 'VACCINATION_DATE', 'VACCINE_PRODUCT', 'VACCINE_DOSE')
vaccine.createOrReplaceGlobalTempView(f'{project_prefix}vaccine_covid')
# global_temp.{project_prefix}vaccine_covid


# COMMAND ----------

display(vaccine)

# COMMAND ----------

drop_table(project_prefix + vaccine_dates_table, if_exists = True)

# COMMAND ----------

create_table(project_prefix + vaccine_dates_table , select_sql_script=f"SELECT * FROM global_temp.ccu035_01_vaccine_covid") 

# COMMAND ----------



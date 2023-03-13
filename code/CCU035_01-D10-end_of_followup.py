# Databricks notebook source
# MAGIC 
# MAGIC %md # CCU035_01-D11-End_of_followup
# MAGIC  
# MAGIC **Description** Generate end of follow-up date using death date, first vaccine date, and current date.
# MAGIC  
# MAGIC **Author(s)** Hannah Whittaker
# MAGIC 
# MAGIC **Project(s)** CCU035_01
# MAGIC 
# MAGIC **Reviewer(s)** 
# MAGIC  
# MAGIC **Date last updated** 24-03-2022
# MAGIC  
# MAGIC **Date last reviewed** UNREVIEWED
# MAGIC  
# MAGIC **Date last run** 
# MAGIC  
# MAGIC **Data input** 
# MAGIC 
# MAGIC **Data output**
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Software and versions** SQL, Python
# MAGIC  
# MAGIC **Packages and versions** Not applicable

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

# MAGIC %run "/Workspaces/dars_nic_391419_j3w9t_collab/SHDS/common/functions"

# COMMAND ----------

# MAGIC %run "/Workspaces/dars_nic_391419_j3w9t_collab/CCU035_01/CCU035_01-functions/covariate_functions"

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Parameters

# COMMAND ----------

#Dataset parameters
project_prefix = 'ccu035_01_'
collab_database_name = 'dars_nic_391419_j3w9t_collab'
covid_index_date = 'covid_index_date'
included_patients = 'inf_included_patients'
vaccine_data = 'vac_vaccine_status'

#Date parameters 
current_date = '2022-03-16'

#Final table name
final_table = 'covid_index_date_end_followup_date'


# COMMAND ----------

covid = spark.table(f'{collab_database_name}.{project_prefix}covid_index_date')\
  .withColumnRenamed('person_id_deid', 'NHS_NUMBER_DEID')

#04/11 changed to vac_vaccine_status to find first one before people were dropped in previous vaccine data file
vaccine = spark.table(f'dars_nic_391419_j3w9t_collab.ccu035_01vaccine_all')

#merge included patients with covid cohort
patients = spark.table(f'{collab_database_name}.{project_prefix}inf_included_patients')\
  .join(covid, on='NHS_NUMBER_DEID', how='inner')
patients.createOrReplaceGlobalTempView(f'{project_prefix}patients_covid')

# COMMAND ----------

display(vaccine)

# COMMAND ----------

vaccine=vaccine

_win_rownum = Window\
  .partitionBy('NHS_NUMBER_DEID')\
  .orderBy(f.asc('vaccination_date'))

#keep relevant variables and keep row with latest height event
vaccine_first=vaccine\
  .withColumn('_rownum', f.row_number().over(_win_rownum))\
  .where(f.col('_rownum') == 1)
vaccine_first.createOrReplaceGlobalTempView(f'ccu035_01_vaccine_first')


# COMMAND ----------

display(vaccine_first)

# COMMAND ----------

count_var(vaccine_first, 'NHS_NUMBER_DEID')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.ccu035_01_patients_covid

# COMMAND ----------

# MAGIC %md
# MAGIC #### Merge in vaccine date 

# COMMAND ----------

spark.sql(F"""
create or replace global temp view {project_prefix}end_date as

with cte_cov as (
SELECT end.NHS_NUMBER_DEID, end.DATE_OF_DEATH, end.covid19_confirmed_date, 
vaccine.vaccination_date

FROM global_temp.{project_prefix}patients_covid as end
LEFT JOIN global_temp.{project_prefix}vaccine_first as vaccine
ON end.NHS_NUMBER_DEID = vaccine.NHS_NUMBER_DEID)

select NHS_NUMBER_DEID, DATE_OF_DEATH, vaccination_date, covid19_confirmed_date

from cte_cov
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.ccu035_01_end_date

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create end of collection date

# COMMAND ----------

df = spark.table(f'global_temp.{project_prefix}end_date')\
.withColumn('lcd', f.to_date(f.lit('2021-11-30')))
df.createOrReplaceGlobalTempView(f'{project_prefix}end_date')
#df.show()


# COMMAND ----------

count_var(df, 'NHS_NUMBER_DEID')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.ccu035_01_end_date

# COMMAND ----------

count_var( 'NHS_NUMBER_DEID')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create 2nd covid infection date

# COMMAND ----------

#We already have this table created which includes all covid infection dates 
covid_infection = spark.table(f'{collab_database_name}.{project_prefix}covid_trajectory')\
  .withColumnRenamed('person_id_deid', 'NHS_NUMBER_DEID')\
  .withColumnRenamed('covid19_confirmed_date', 'date')\
  .where(f.col('covid_phenotype').isin(['01_Covid_positive_test', '01_GP_covid_diagnosis', '02_Covid_admission_any_position', '02_Covid_admission_primary_position']))
covid_infection.createOrReplaceGlobalTempView(f'{project_prefix}covid_trajectory')
  

# COMMAND ----------

tmpt = tab(covid_infection, 'covid_phenotype', 'source')

# COMMAND ----------

display(covid_infection)

# COMMAND ----------

# #merge global end_date table with covid_trajectory table
# spark.sql(F"""
# create or replace global temp view {project_prefix}end_date as

# with cte_cov as (
# SELECT end.NHS_NUMBER_DEID, end.DATE_OF_DEATH, end.vaccine_1_date, end.covid19_confirmed_date, end.lcd,
# covid.date

# FROM global_temp.{project_prefix}end_date as end
# LEFT JOIN global_temp.{project_prefix}covid_trajectory as covid
# ON end.NHS_NUMBER_DEID = covid.NHS_NUMBER_DEID)

# select NHS_NUMBER_DEID, DATE_OF_DEATH, vaccine_1_date, covid19_confirmed_date, date, lcd

# from cte_cov
# """)

# COMMAND ----------

covid_infection_2 = covid_infection\
  .select('NHS_NUMBER_DEID', 'date')

_win_rownum = Window\
  .partitionBy('NHS_NUMBER_DEID')\
  .orderBy(['date'])

covid19_confirmed_date_2 = spark.table(f'global_temp.{project_prefix}end_date')\
  .join(covid_infection_2, on='NHS_NUMBER_DEID', how='left')\
  .withColumn('covid19_confirmed_date_p3m', f.add_months(f.col('covid19_confirmed_date'), 3))\
  .where(f.col('date') > f.col('covid19_confirmed_date_p3m'))\
  .withColumn('_rownum', f.row_number().over(_win_rownum))\
  .where(f.col('_rownum') == 1)\
  .select('NHS_NUMBER_DEID', 'date')\
  .withColumnRenamed('date', 'covid19_confirmed_date_2')\
  .orderBy('NHS_NUMBER_DEID')

# COMMAND ----------

end_date = spark.table(f'global_temp.{project_prefix}end_date')

# COMMAND ----------

count_var(end_date, 'NHS_NUMBER_DEID')
count_var(covid19_confirmed_date_2, 'NHS_NUMBER_DEID')

# COMMAND ----------

end_date = end_date\
  .join(covid19_confirmed_date_2, on='NHS_NUMBER_DEID', how='left')

# COMMAND ----------

count_var(end_date, 'NHS_NUMBER_DEID')

# COMMAND ----------

end_date = end_date\
  .withColumn('min_date', f.least('DATE_OF_DEATH', 'vaccination_date', 'lcd', 'covid19_confirmed_date_2'))\
  .withColumnRenamed('min_date', 'end_date')
end_date.createOrReplaceGlobalTempView(f'{project_prefix}end_date')

# COMMAND ----------

display(end_date)

# COMMAND ----------

count_var(end_date, 'NHS_NUMBER_DEID')

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table  dars_nic_391419_j3w9t_collab.ccu035_01_start_and_end_date

# COMMAND ----------

# MAGIC %sql
# MAGIC create table dars_nic_391419_j3w9t_collab.ccu035_01_start_and_end_date as
# MAGIC select *
# MAGIC from global_temp.ccu035_01_end_date

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dars_nic_391419_j3w9t_collab.ccu035_01_start_and_end_date

# COMMAND ----------

#21/09/22- just need 2nd covid infection date to import into stata
second_infection=end_date\
  .select('nhs_number_deid', 'covid19_confirmed_date_2')
second_infection.createOrReplaceGlobalTempView(f'{project_prefix}second_infection')
display(second_infection)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table dars_nic_391419_j3w9t_collab.ccu035_01_second_infection as
# MAGIC select NHS_NUMBER_DEID, covid19_confirmed_date_2
# MAGIC from global_temp.ccu035_01_second_infection

# COMMAND ----------



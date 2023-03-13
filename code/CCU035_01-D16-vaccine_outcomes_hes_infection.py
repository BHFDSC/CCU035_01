# Databricks notebook source
# MAGIC %md #HES APC: CVD outcomes (work package 2.5)
# MAGIC **Description** This notebooks make a table CCU035_01_cvd_outcomes_hesapc which contains all observations with the relevant observations containing outcomes clinical codes from the HES APC dataset recorded from **covid19_confirmed_date** until end of follow-up. Phenotypes used are listed below:
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
# MAGIC **Data input** HES APC
# MAGIC 
# MAGIC **Data output table:** CCU035_01_cvd_outcomes_hesapc
# MAGIC 
# MAGIC **Software and versions** SQL
# MAGIC 
# MAGIC **Packages and versions** 'Not applicable'

# COMMAND ----------

# MAGIC %md
# MAGIC #####Infection parameters

# COMMAND ----------

# MAGIC %py
# MAGIC 
# MAGIC #Dataset parameters
# MAGIC hes_data = 'dars_nic_391419_j3w9t_collab.ccu035_01_hes_apc_all_years'
# MAGIC index_dates='dars_nic_391419_j3w9t_collab.ccu035_01_start_and_end_date'
# MAGIC project_prefix = 'ccu035_01_'
# MAGIC collab_database_name = 'dars_nic_391419_j3w9t_collab'
# MAGIC 
# MAGIC 
# MAGIC  

# COMMAND ----------

# Define create table function by Sam Hollings
# Source: Workspaces/dars_nic_391419_j3w9t_collab/DATA_CURATION_wrang000_functions

def create_table(table_name:str, database_name:str='dars_nic_391419_j3w9t_collab', select_sql_script:str=None) -> None:
  """Will save to table from a global_temp view of the same name as the supplied table name (if no SQL script is supplied)
  Otherwise, can supply a SQL script and this will be used to make the table with the specificed name, in the specifcied database."""
  
  spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
  
  if select_sql_script is None:
    select_sql_script = f"SELECT * FROM global_temp.{table_name}"
  
  spark.sql(f"""CREATE TABLE {database_name}.{table_name} AS
                {select_sql_script}
             """)
  spark.sql(f"ALTER TABLE {database_name}.{table_name} OWNER TO {database_name}")
  
def drop_table(table_name:str, database_name:str='dars_nic_391419_j3w9t_collab', if_exists=True):
  if if_exists:
    IF_EXISTS = 'IF EXISTS'
  else: 
    IF_EXISTS = ''
  spark.sql(f"DROP TABLE {IF_EXISTS} {database_name}.{table_name}")

# COMMAND ----------

# MAGIC %py
# MAGIC 
# MAGIC spark.sql(F"""create or replace global temp view CCU035_01_infection_hesapc
# MAGIC as select *
# MAGIC from {hes_data}""") 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.CCU035_01_infection_hesapc

# COMMAND ----------

# MAGIC %sql
# MAGIC ----All relevent HES outcome codelists used
# MAGIC 
# MAGIC create or replace global temp view CCU035_01_infection_hesoutcomes_map as
# MAGIC 
# MAGIC SELECT LEFT ( REGEXP_REPLACE(code,'[.,-,' ']','') , 4 ) AS ICD10code_trunc,
# MAGIC name, terminology, code, term, code_type, RecordDate
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu035_01_codelist
# MAGIC WHERE (name = 'AMI' OR name = 'HF' OR name = 'angina' OR name = 'stroke_TIA' OR name = 'stroke_isch' OR name = 'unstable_angina' OR name = 'DIC' OR name = 'DVT_DVT' OR name = 'DVT_ICVT' OR name = 'DVT_pregnancy' OR name = 'ICVT_pregnancy' OR name = 'PE' OR name = 'TTP' OR name = 'artery_dissect' OR name = 'cardiomyopathy' OR name = 'fracture' OR name = 'life_arrhythmia' OR name = 'mesenteric_thrombus' OR name = 'myocarditis' OR name = 'other_DVT' OR name = 'other_arterial_embolism' OR name = 'pericarditis' OR name = 'portal_vein_thrombosis' OR name = 'stroke_SAH_HS' OR name = 'thrombocytopenia' OR name = 'thrombophilia')
# MAGIC       AND terminology = 'ICD10' --AND code_type=1 AND RecordDate=20210127
# MAGIC       

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Available outcomes in the GDPPR dataset
# MAGIC 
# MAGIC select name
# MAGIC from global_temp.CCU035_01_infection_hesoutcomes_map
# MAGIC group by name

# COMMAND ----------

# MAGIC %md 
# MAGIC ###First position

# COMMAND ----------

index_dates=spark.table (f'dars_nic_391419_j3w9t_collab.ccu035_01_start_and_end_date')

# COMMAND ----------

display(index_dates)

# COMMAND ----------

#merge in end date frin D11 worksapce -HW 
hesapc = spark.table(f'global_temp.{project_prefix}infection_hesapc')\
  .withColumnRenamed('person_id_deid', 'NHS_NUMBER_DEID')\
  .join(index_dates, on='NHS_NUMBER_DEID', how='inner')
hesapc.createOrReplaceGlobalTempView(f'{project_prefix}infection_hesapc')



# COMMAND ----------

#%sql
#drop table  dars_nic_391419_j3w9t_collab.ccu035_01_infection_hesapcoutcomes_first_diagnosis 

# COMMAND ----------

# MAGIC %sql
# MAGIC -------Patients with relevant CVD event codes in the HES APC dataset after covid infection and before end of follow-up
# MAGIC ---created truncated caliber codes with dot missing and using all outcomes
# MAGIC 
# MAGIC CREATE TABLE dars_nic_391419_j3w9t_collab.CCU035_01_infection_hesapcoutcomes_first_diagnosis_all  AS
# MAGIC --create or replace global temp view CCU035_01_infection_hesapcoutcomes_first_diagnosis as
# MAGIC 
# MAGIC with cte_hes as (
# MAGIC SELECT NHS_NUMBER_DEID, SPELBGIN, EPISTART, ADMIDATE, DISDATE, DIAG_3_CONCAT, DIAG_3_01, DIAG_3_02, 
# MAGIC DIAG_3_03, DIAG_3_04, DIAG_3_05, DIAG_3_06, DIAG_3_07, DIAG_3_08, DIAG_3_09, DIAG_3_10, 
# MAGIC DIAG_3_11, DIAG_3_12, DIAG_3_13, DIAG_3_14, DIAG_3_15, DIAG_3_16, DIAG_3_17, DIAG_3_18, 
# MAGIC DIAG_3_19, DIAG_3_20, 
# MAGIC DIAG_4_CONCAT, DIAG_4_01, DIAG_4_02, 
# MAGIC DIAG_4_03, DIAG_4_04, DIAG_4_05, DIAG_4_06, DIAG_4_07, DIAG_4_08, DIAG_4_09, DIAG_4_10, 
# MAGIC DIAG_4_11, DIAG_4_12, DIAG_4_13, DIAG_4_14, DIAG_4_15, DIAG_4_16, DIAG_4_17, DIAG_4_18, 
# MAGIC DIAG_4_19, DIAG_4_20, covid19_confirmed_date, end_date,
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_01,'[X]$','') , 4 ) AS DIAG_4_01_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_02,'[X]$','') , 4 ) AS DIAG_4_02_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_03,'[X]$','') , 4 ) AS DIAG_4_03_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_04,'[X]$','') , 4 ) AS DIAG_4_04_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_05,'[X]$','') , 4 ) AS DIAG_4_05_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_06,'[X]$','') , 4 ) AS DIAG_4_06_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_07,'[X]$','') , 4 ) AS DIAG_4_07_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_08,'[X]$','') , 4 ) AS DIAG_4_08_trunc, 
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_09,'[X]$','') , 4 ) AS DIAG_4_09_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_10,'[X]$','') , 4 ) AS DIAG_4_10_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_11,'[X]$','') , 4 ) AS DIAG_4_11_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_12,'[X]$','') , 4 ) AS DIAG_4_12_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_13,'[X]$','') , 4 ) AS DIAG_4_13_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_14,'[X]$','') , 4 ) AS DIAG_4_14_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_15,'[X]$','') , 4 ) AS DIAG_4_15_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_16,'[X]$','') , 4 ) AS DIAG_4_16_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_17,'[X]$','') , 4 ) AS DIAG_4_17_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_18,'[X]$','') , 4 ) AS DIAG_4_18_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_19,'[X]$','') , 4 ) AS DIAG_4_19_trunc,
# MAGIC  LEFT ( REGEXP_REPLACE(DIAG_4_20,'[X]$','') , 4 ) AS DIAG_4_20_trunc
# MAGIC FROM global_temp.CCU035_01_infection_hesapc
# MAGIC WHERE EPISTART > covid19_confirmed_date 
# MAGIC )
# MAGIC 
# MAGIC select *
# MAGIC from cte_hes t1
# MAGIC inner join global_temp.CCU035_01_infection_hesoutcomes_map t2 on 
# MAGIC   (t1.DIAG_3_01 = t2.ICD10code_trunc
# MAGIC /*OR t1.DIAG_3_02 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_3_03 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_3_04 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_3_05 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_3_06 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_3_07 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_3_08 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_3_09 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_3_10 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_3_11 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_3_12 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_3_13 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_3_14 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_3_15 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_3_16 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_3_17 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_3_18 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_3_19 = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_3_20 = t2.ICD10code_trunc*/
# MAGIC OR t1.DIAG_4_01_trunc = t2.ICD10code_trunc
# MAGIC /*OR t1.DIAG_4_02_trunc = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_03_trunc = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_04_trunc = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_05_trunc = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_06_trunc = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_07_trunc = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_08_trunc = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_09_trunc = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_10_trunc = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_11_trunc = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_12_trunc = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_13_trunc = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_14_trunc = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_15_trunc = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_16_trunc = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_17_trunc = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_18_trunc = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_19_trunc = t2.ICD10code_trunc
# MAGIC OR t1.DIAG_4_20_trunc = t2.ICD10code_trunc*/
# MAGIC )

# COMMAND ----------

#%sql
#---Number of patients with specific conditions
#SELECT count(Distinct NHS_NUMBER_DEID) as count, name
#--FROM global_temp.CCU035_01_infection_hesapcoutcomes_first_diagnosis 
#FROM dars_nic_391419_j3w9t_collab.CCU035_01_infection_hesapcoutcomes_first_diagnosis
#group by name
#order by count desc 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(Distinct NHS_NUMBER_DEID) as count
# MAGIC FROM dars_nic_391419_j3w9t_collab.CCU035_01_infection_hesapcoutcomes_first_diagnosis_all

# COMMAND ----------

# MAGIC %md 
# MAGIC ###All diagnosis positions

# COMMAND ----------

"""%sql
-------Patients with relevant CVD event codes in the HES APC dataset after 31st Jan 2020
---created truncated caliber codes with dot missing and using all outcomes

CREATE TABLE dars_nic_391419_j3w9t_collab.CCU035_01_infection_hesapcoutcomes_  AS
--create or replace global temp view CCU035_01_infection_hesapcoutcomes as

with cte_hes as (
SELECT NHS_NUMBER_DEID, SPELBGIN, EPISTART, ADMIDATE, DISDATE, DIAG_3_CONCAT, DIAG_3_01, DIAG_3_02, 
DIAG_3_03, DIAG_3_04, DIAG_3_05, DIAG_3_06, DIAG_3_07, DIAG_3_08, DIAG_3_09, DIAG_3_10, 
DIAG_3_11, DIAG_3_12, DIAG_3_13, DIAG_3_14, DIAG_3_15, DIAG_3_16, DIAG_3_17, DIAG_3_18, 
DIAG_3_19, DIAG_3_20, 
DIAG_4_CONCAT, DIAG_4_01, DIAG_4_02, 
DIAG_4_03, DIAG_4_04, DIAG_4_05, DIAG_4_06, DIAG_4_07, DIAG_4_08, DIAG_4_09, DIAG_4_10, 
DIAG_4_11, DIAG_4_12, DIAG_4_13, DIAG_4_14, DIAG_4_15, DIAG_4_16, DIAG_4_17, DIAG_4_18, 
DIAG_4_19, DIAG_4_20, covid19_confirmed_date, end_date,
 LEFT ( REGEXP_REPLACE(DIAG_4_01,'[X]$','') , 4 ) AS DIAG_4_01_trunc,
 LEFT ( REGEXP_REPLACE(DIAG_4_02,'[X]$','') , 4 ) AS DIAG_4_02_trunc,
 LEFT ( REGEXP_REPLACE(DIAG_4_03,'[X]$','') , 4 ) AS DIAG_4_03_trunc,
 LEFT ( REGEXP_REPLACE(DIAG_4_04,'[X]$','') , 4 ) AS DIAG_4_04_trunc,
 LEFT ( REGEXP_REPLACE(DIAG_4_05,'[X]$','') , 4 ) AS DIAG_4_05_trunc,
 LEFT ( REGEXP_REPLACE(DIAG_4_06,'[X]$','') , 4 ) AS DIAG_4_06_trunc,
 LEFT ( REGEXP_REPLACE(DIAG_4_07,'[X]$','') , 4 ) AS DIAG_4_07_trunc,
 LEFT ( REGEXP_REPLACE(DIAG_4_08,'[X]$','') , 4 ) AS DIAG_4_08_trunc, 
 LEFT ( REGEXP_REPLACE(DIAG_4_09,'[X]$','') , 4 ) AS DIAG_4_09_trunc,
 LEFT ( REGEXP_REPLACE(DIAG_4_10,'[X]$','') , 4 ) AS DIAG_4_10_trunc,
 LEFT ( REGEXP_REPLACE(DIAG_4_11,'[X]$','') , 4 ) AS DIAG_4_11_trunc,
 LEFT ( REGEXP_REPLACE(DIAG_4_12,'[X]$','') , 4 ) AS DIAG_4_12_trunc,
 LEFT ( REGEXP_REPLACE(DIAG_4_13,'[X]$','') , 4 ) AS DIAG_4_13_trunc,
 LEFT ( REGEXP_REPLACE(DIAG_4_14,'[X]$','') , 4 ) AS DIAG_4_14_trunc,
 LEFT ( REGEXP_REPLACE(DIAG_4_15,'[X]$','') , 4 ) AS DIAG_4_15_trunc,
 LEFT ( REGEXP_REPLACE(DIAG_4_16,'[X]$','') , 4 ) AS DIAG_4_16_trunc,
 LEFT ( REGEXP_REPLACE(DIAG_4_17,'[X]$','') , 4 ) AS DIAG_4_17_trunc,
 LEFT ( REGEXP_REPLACE(DIAG_4_18,'[X]$','') , 4 ) AS DIAG_4_18_trunc,
 LEFT ( REGEXP_REPLACE(DIAG_4_19,'[X]$','') , 4 ) AS DIAG_4_19_trunc,
 LEFT ( REGEXP_REPLACE(DIAG_4_20,'[X]$','') , 4 ) AS DIAG_4_20_trunc
FROM global_temp.CCU035_01_infection_hesapc
WHERE EPISTART > covid19_confirmed_date 
)

select *
from cte_hes t1
inner join global_temp.CCU035_01_infection_hesoutcomes_map t2 on 
  (t1.DIAG_3_01 = t2.ICD10code_trunc
OR t1.DIAG_3_02 = t2.ICD10code_trunc
OR t1.DIAG_3_03 = t2.ICD10code_trunc
OR t1.DIAG_3_04 = t2.ICD10code_trunc
OR t1.DIAG_3_05 = t2.ICD10code_trunc
OR t1.DIAG_3_06 = t2.ICD10code_trunc
OR t1.DIAG_3_07 = t2.ICD10code_trunc
OR t1.DIAG_3_08 = t2.ICD10code_trunc
OR t1.DIAG_3_09 = t2.ICD10code_trunc
OR t1.DIAG_3_10 = t2.ICD10code_trunc
OR t1.DIAG_3_11 = t2.ICD10code_trunc
OR t1.DIAG_3_12 = t2.ICD10code_trunc
OR t1.DIAG_3_13 = t2.ICD10code_trunc
OR t1.DIAG_3_14 = t2.ICD10code_trunc
OR t1.DIAG_3_15 = t2.ICD10code_trunc
OR t1.DIAG_3_16 = t2.ICD10code_trunc
OR t1.DIAG_3_17 = t2.ICD10code_trunc
OR t1.DIAG_3_18 = t2.ICD10code_trunc
OR t1.DIAG_3_19 = t2.ICD10code_trunc
OR t1.DIAG_3_20 = t2.ICD10code_trunc
OR t1.DIAG_4_01_trunc = t2.ICD10code_trunc
OR t1.DIAG_4_02_trunc = t2.ICD10code_trunc
OR t1.DIAG_4_03_trunc = t2.ICD10code_trunc
OR t1.DIAG_4_04_trunc = t2.ICD10code_trunc
OR t1.DIAG_4_05_trunc = t2.ICD10code_trunc
OR t1.DIAG_4_06_trunc = t2.ICD10code_trunc
OR t1.DIAG_4_07_trunc = t2.ICD10code_trunc
OR t1.DIAG_4_08_trunc = t2.ICD10code_trunc
OR t1.DIAG_4_09_trunc = t2.ICD10code_trunc
OR t1.DIAG_4_10_trunc = t2.ICD10code_trunc
OR t1.DIAG_4_11_trunc = t2.ICD10code_trunc
OR t1.DIAG_4_12_trunc = t2.ICD10code_trunc
OR t1.DIAG_4_13_trunc = t2.ICD10code_trunc
OR t1.DIAG_4_14_trunc = t2.ICD10code_trunc
OR t1.DIAG_4_15_trunc = t2.ICD10code_trunc
OR t1.DIAG_4_16_trunc = t2.ICD10code_trunc
OR t1.DIAG_4_17_trunc = t2.ICD10code_trunc
OR t1.DIAG_4_18_trunc = t2.ICD10code_trunc
OR t1.DIAG_4_19_trunc = t2.ICD10code_trunc
OR t1.DIAG_4_20_trunc = t2.ICD10code_trunc
)
"""

# COMMAND ----------

"""
%sql
---Number of patients with specific conditions
SELECT count(Distinct NHS_NUMBER_DEID) as count, name
--FROM global_temp.CCU035_01_infection_hesapcoutcomes_first_diagnosis 
FROM dars_nic_391419_j3w9t_collab.CCU035_01_infection_hesapcoutcomes
group by name
order by count desc 
"""

# COMMAND ----------

#%sql
#SELECT count(Distinct NHS_NUMBER_DEID) as count
#FROM dars_nic_391419_j3w9t_collab.CCU035_01_infection_hesapcoutcomes

# COMMAND ----------

#%sql
#select * from dars_nic_391419_j3w9t_collab.CCU035_01_infection_hesapcoutcomes

# Databricks notebook source
# MAGIC %md
# MAGIC  # CCU035_01-D09-COVID_trajectory
# MAGIC  
# MAGIC **Description** 
# MAGIC 
# MAGIC This notebook:
# MAGIC * Identifies all patients with COVID-19 related diagnosis
# MAGIC * Creates a *trajectory* table with all data points for all affected individuals
# MAGIC * Creates a *severity* table where all individuals are assigned a mutually-exclusive COVID-19 severity phenotype (mild, moderate, severe, death) based on the worst event they experience  
# MAGIC   
# MAGIC NB:
# MAGIC * start and stop dates for the phenotyping is defined in notebook `CCU035_01-D08-COVID_infections_temp_tables`
# MAGIC 
# MAGIC **Project(s)** CCU035_01
# MAGIC  
# MAGIC **Author(s)** Johan Thygesen, Chris Tomlinson, Spiros Denaxas
# MAGIC  
# MAGIC **Reviewer(s)** 
# MAGIC  
# MAGIC **Date last updated** 
# MAGIC  
# MAGIC **Date last reviewed** 
# MAGIC  
# MAGIC **Date last run** 
# MAGIC  
# MAGIC **Data input**  
# MAGIC All inputs are via notebook `CCU035_01-D08-COVID_infections_temp_tables`
# MAGIC We also source functions from `CCU013_01_helper_functions`
# MAGIC 
# MAGIC **Data output**
# MAGIC 
# MAGIC * `ccu035_01_covid_trajectory`
# MAGIC * `ccu035_01_covid_severity`
# MAGIC 
# MAGIC **Software and versions** SQL, python
# MAGIC  
# MAGIC **Packages and versions** See cell below:

# COMMAND ----------

# MAGIC %run Workspaces/dars_nic_391419_j3w9t_collab/CCU035_01/CCU035_01-functions/wrang000_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load functions and input data

# COMMAND ----------

# MAGIC %run "/Workspaces/dars_nic_391419_j3w9t_collab/CCU035_01/CCU035_01-D15-master_notebook_parameters"

# COMMAND ----------

#Dataset parameters
project_prefix = 'ccu035_01_'
collab_database_name = 'dars_nic_391419_j3w9t_collab' 
gdppr_data = 'gdppr_dars_nic_391419_j3w9t'
hes_apc_data = 'hes_apc_all_years'
hes_op_data = 'hes_op_all_years'
hes_ae_data = 'hes_ae_all_years'
hes_cc_data = 'hes_cc_all_years'
deaths_data = 'deaths_dars_nic_391419_j3w9t'
sgss_data = 'sgss_dars_nic_391419_j3w9t'
chess_data = 'chess_dars_nic_391419_j3w9t'
sus_data = 'sus_dars_nic_391419_j3w9t'
codelist_final_name = 'codelist'


#Date parameters
index_date = '2020-01-01' 

#Temp tables
temp_sgss = 'tmp_sgss'
temp_gdppr = 'tmp_gdppr'
temp_deaths = 'tmp_deaths'
temp_hes_apc = 'tmp_hes_apc'
temp_hes_cc = 'tmp_hes_cc'
temp_chess = 'tmp_chess'
temp_sus = 'tmp_sus'



#Final tables
temp_trajectory = 'tmp_covid_trajectory'
trajectory_final = 'covid_trajectory'





# COMMAND ----------

from pyspark.sql.functions import lit, col, udf
from functools import reduce
from pyspark.sql import DataFrame
from datetime import datetime
from pyspark.sql.types import DateType

# COMMAND ----------

# To reload the creation of the global temp tables run this line
dbutils.widgets.removeAll()

# COMMAND ----------

# Load global temp tables used to identify COVID-19 events
# NB no longer global temp views but 'proper tables' therefore better to run separately as takes ~40 mins
# dbutils.notebook.run("./CCU013_01_create_table_aliases", 30000) # timeout_seconds

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU035_01/CCU035_01-functions/CCU035_01_COVID_trajectory_helper_functions_from_CCU013

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Identify all patients with COVID19
# MAGIC 
# MAGIC Create a single table (**`ccu013_covid_trajectories`**) that contains all COVID-19 events from all input tables (SGSS, GDPPR, HES_APC.. etc) with the following format.
# MAGIC 
# MAGIC |Column | Content |
# MAGIC |----------------|--------------------|
# MAGIC |patient_id_deid| Patient NHS Number |
# MAGIC |date | Date of event: date in GDPPR, speciment date SGSS, epistart HES |
# MAGIC |covid_phenotype | Cateogrical: Positive PCR test; Confirmed_COVID19; Suspected_COVID19; Lab confirmed incidence; Lab confirmed historic; Lab confirmed unclear; Clinically confirmed |
# MAGIC |clinical_code | Reported clinical code |
# MAGIC |description | Description of the clinical code if relevant|
# MAGIC |code | Type of code: ICD10; SNOMED |
# MAGIC |source | Source from which the data was drawn: SGSS; HES APC; Primary care |
# MAGIC |date_is | Original column name of date |

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.1: COVID positive and diagnosis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Pillar 2 table
# MAGIC -- Not included currently as there are some data issues! (table has been withdrawn)
# MAGIC -- The following codes are negative: 1322791000000100, 1240591000000102
# MAGIC -- The following codes are unknown:  1321691000000102, 1322821000000105
# MAGIC --- NOTE: The inclusion of only positive tests have already been done in notebook ccu013_create_table_aliase
# MAGIC 
# MAGIC --CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_pillar2_covid
# MAGIC --AS
# MAGIC --SELECT person_id_deid, date, 
# MAGIC -- Decision to group all pillar 2 tests together as distinguishing lateral flow vs PCR not required for severity phenotyping
# MAGIC --"01_Covid_positive_test" as covid_phenotype, 
# MAGIC --TestResult as clinical_code, 
# MAGIC --CASE WHEN TestResult = '1322781000000102' THEN "Severe acute respiratory syndrome coronavirus 2 antigen detection result positive (finding)" 
# MAGIC --WHEN TestResult = '1240581000000104' THEN "Severe acute respiratory syndrome coronavirus 2 ribonucleic acid detected (finding)" else NULL END as description,
# MAGIC --'confirmed' as covid_status,
# MAGIC --"SNOMED" as code,
# MAGIC --source, date_is
# MAGIC --FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_pillar2

# COMMAND ----------

#SGSS table
#all records are included as every record is a "positive test"
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}sgss_covid
AS
SELECT person_id_deid, date, 
"01_Covid_positive_test" as covid_phenotype, 
"" as clinical_code, 
CASE WHEN REPORTING_LAB_ID = '840' THEN "pillar_2" ELSE "pillar_1" END as description,
"confirmed" as covid_status,
"" as code,
"SGSS" as source, date_is
FROM {collab_database_name}.{project_prefix}{temp_sgss} """)

# COMMAND ----------

#GDPPR 
#Only includes individuals with a COVID SNOMED CODE
#SNOMED CODES are defined in: CCU013_01_create_table_aliases
#Optimisation =  /*+ BROADCAST(tab1) */ -- forces it to send a copy of the small table to each worker

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}gdppr_covid as
with cte_snomed_covid_codes as (
select code, term
from {collab_database_name}.{project_prefix}{codelist_final_name}
where name like 'GDPPR_confirmed_COVID' ),

cte_gdppr as (
SELECT tab2.person_id_deid, tab2.date, tab2.code, tab2.date_is, tab1.term
FROM cte_snomed_covid_codes tab1
inner join {collab_database_name}.{project_prefix}{temp_gdppr} tab2 on tab1.code = tab2.code
)

SELECT person_id_deid, date, 
"01_GP_covid_diagnosis" as covid_phenotype,
code as clinical_code, 
term as description,
"confirmed" as covid_status, --- NEED To inspect and identify which are only suspected!
"SNOMED" as code, 
"GDPPR" as source, date_is 
from cte_gdppr """)

# COMMAND ----------

# MAGIC %sql
# MAGIC --- HES Dropped as source since 080621
# MAGIC --- HES_OP
# MAGIC --- Get all patients hospitalised with a covid diagnosis U07.1 or U07.2
# MAGIC 
# MAGIC --CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_op_covid as
# MAGIC --SELECT person_id_deid, date, 
# MAGIC --"01_GP_covid_diagnosis" as covid_phenotype,
# MAGIC --(case when DIAG_4_CONCAT LIKE "%U071%" THEN 'U07.1'
# MAGIC --when DIAG_4_CONCAT LIKE "%U072%" THEN 'U07.2' Else '0' End) as clinical_code,
# MAGIC --(case when DIAG_4_CONCAT LIKE "%U071%" THEN 'Confirmed_COVID19'
# MAGIC --when DIAG_4_CONCAT LIKE "%U072%" THEN 'Suspected_COVID19' Else '0' End) as description,
# MAGIC --(case when DIAG_4_CONCAT LIKE "%U071%" THEN 'confirmed'
# MAGIC --when DIAG_4_CONCAT LIKE "%U072%" THEN 'suspected' Else '0' End) as covid_status,
# MAGIC --"ICD10" as code,
# MAGIC --"HES OP" as source, 
# MAGIC --date_is
# MAGIC --FROM dars_nic_391419_j3w9t_collab.ccu013_tmp_op
# MAGIC --WHERE DIAG_4_CONCAT LIKE "%U071%"
# MAGIC --   OR DIAG_4_CONCAT LIKE "%U072%"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2: Covid Admission

# COMMAND ----------

#SUS - Hospitalisations (COVID in any position)
#Get all patients hospitalised with a covid diagnosis U07.1 or U07.2
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}sus_covid_any_position as
SELECT person_id_deid, date, 
"02_Covid_admission_any_position" as covid_phenotype,
(case when DIAG_CONCAT LIKE "%U071%" THEN 'U07.1'
when DIAG_CONCAT LIKE "%U072%" THEN 'U07.2' Else '0' End) as clinical_code,
(case when DIAG_CONCAT LIKE "%U071%" THEN 'Confirmed_COVID19'
when DIAG_CONCAT LIKE "%U072%" THEN 'Suspected_COVID19' Else '0' End) as description,
(case when DIAG_CONCAT LIKE "%U071%" THEN 'confirmed'
when DIAG_CONCAT LIKE "%U072%" THEN 'suspected' Else '0' End) as covid_status,
"ICD10" as code,
"SUS" as source, 
date_is
FROM {collab_database_name}.{project_prefix}{temp_sus}
WHERE DIAG_CONCAT LIKE "%U071%"
   OR DIAG_CONCAT LIKE "%U072%" """)

# COMMAND ----------

#SUS - Hospitalisations (COVID in primary position)
#Get all patients hospitalised with a covid diagnosis U07.1 or U07.2
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}sus_covid_primary_position as
SELECT person_id_deid, date, 
"02_Covid_admission_primary_position" as covid_phenotype,
(case when PRIMARY_DIAGNOSIS_CODE LIKE "%U071%" THEN 'U07.1'
when PRIMARY_DIAGNOSIS_CODE LIKE "%U072%" THEN 'U07.2' Else '0' End) as clinical_code,
(case when PRIMARY_DIAGNOSIS_CODE LIKE "%U071%" THEN 'Confirmed_COVID19'
when PRIMARY_DIAGNOSIS_CODE LIKE "%U072%" THEN 'Suspected_COVID19' Else '0' End) as description,
(case when PRIMARY_DIAGNOSIS_CODE LIKE "%U071%" THEN 'confirmed'
when PRIMARY_DIAGNOSIS_CODE LIKE "%U072%" THEN 'suspected' Else '0' End) as covid_status,
"ICD10" as code,
"SUS" as source, 
date_is
FROM {collab_database_name}.{project_prefix}{temp_sus}
WHERE PRIMARY_DIAGNOSIS_CODE LIKE "%U071%"
   OR PRIMARY_DIAGNOSIS_CODE LIKE "%U072%" """)

# COMMAND ----------

#CHESS - Hospitalisations
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}chess_covid_hospital as
SELECT person_id_deid, HospitalAdmissionDate as date,
"02_Covid_admission_any_position" as covid_phenotype,
"" as clinical_code, 
"HospitalAdmissionDate IS NOT null" as description,
"confirmed" as covid_status,
"CHESS" as source, 
"" as code,
"HospitalAdmissionDate" as date_is
FROM {collab_database_name}.{project_prefix}{temp_chess} """)

# COMMAND ----------

#HES_APC - Hospitalisations (COVID in any position)
#Get all patients hospitalised with a covid diagnosis U07.1 or U07.2
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}apc_covid_any_position as
SELECT person_id_deid, date, 
"02_Covid_admission_any_position" as covid_phenotype,
(case when DIAG_4_CONCAT LIKE "%U071%" THEN 'U07.1'
when DIAG_4_CONCAT LIKE "%U072%" THEN 'U07.2' Else '0' End) as clinical_code,
(case when DIAG_4_CONCAT LIKE "%U071%" THEN 'Confirmed_COVID19'
when DIAG_4_CONCAT LIKE "%U072%" THEN 'Suspected_COVID19' Else '0' End) as description,
(case when DIAG_4_CONCAT LIKE "%U071%" THEN 'confirmed'
when DIAG_4_CONCAT LIKE "%U072%" THEN 'suspected' Else '0' End) as covid_status,
"HES APC" as source, 
"ICD10" as code, date_is, SUSRECID
FROM {collab_database_name}.{project_prefix}{temp_hes_apc} """)

# COMMAND ----------

#HES_APC - Hospitalisations (COVID in primary position)
#Get all patients hospitalised with a covid diagnosis U07.1 or U07.2
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}apc_covid_primary_position as
SELECT person_id_deid, date, 
"02_Covid_admission_primary_position" as covid_phenotype,
(case when  DIAG_4_01 LIKE "%U071%" THEN 'U07.1'
when  DIAG_4_01 LIKE "%U072%" THEN 'U07.2' Else '0' End) as clinical_code,
(case when  DIAG_4_01 LIKE "%U071%" THEN 'Confirmed_COVID19'
when  DIAG_4_01 LIKE "%U072%" THEN 'Suspected_COVID19' Else '0' End) as description,
(case when  DIAG_4_01 LIKE "%U071%" THEN 'confirmed'
when  DIAG_4_01 LIKE "%U072%" THEN 'suspected' Else '0' End) as covid_status,
"HES APC" as source, 
"ICD10" as code, date_is, SUSRECID
FROM {collab_database_name}.{project_prefix}{temp_hes_apc} """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3: Critical Care

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.3.1 ICU admission

# COMMAND ----------

#CHESS - ICU
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}chess_covid_icu as
SELECT person_id_deid, DateAdmittedICU as date,
"03_ICU_admission" as covid_phenotype,
"" as clinical_code, 
"DateAdmittedICU IS NOT null" as description,
"confirmed" as covid_status,
"CHESS" as source, 
"" as code,
"DateAdmittedICU" as date_is
FROM {collab_database_name}.{project_prefix}{temp_chess}
WHERE DateAdmittedICU IS NOT null """)

# COMMAND ----------

#HES_CC
#ID is in HES_CC AND has U071 or U072 from HES_APC 
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}cc_covid as
SELECT apc.person_id_deid, cc.date,
'03_ICU_admission' as covid_phenotype,
"" as clinical_code,
"id is in hes_cc table" as description,
"confirmed" as covid_status,
"" as code,
'HES CC' as source, cc.date_is, BRESSUPDAYS, ARESSUPDAYS
FROM {collab_database_name}.{project_prefix}{temp_hes_apc} as apc
INNER JOIN {collab_database_name}.{project_prefix}{temp_hes_cc} AS cc
ON cc.SUSRECID = apc.SUSRECID
WHERE cc.BESTMATCH = 1
AND (DIAG_4_CONCAT LIKE '%U071%' OR DIAG_4_CONCAT LIKE '%U072%') """)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.3.2 NIV

# COMMAND ----------

#CHESS - NIV
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}chess_covid_niv as
SELECT person_id_deid, HospitalAdmissionDate as date,
"03_NIV_treatment" as covid_phenotype,
"" as clinical_code, 
"Highflownasaloxygen OR NoninvasiveMechanicalventilation == Yes" as description,
"confirmed" as covid_status,
"CHESS" as source, 
"" as code,
"HospitalAdmissionDate" as date_is -- Can't be any more precise
FROM {collab_database_name}.{project_prefix}{temp_chess}
WHERE HospitalAdmissionDate IS NOT null
AND (Highflownasaloxygen == "Yes" OR NoninvasiveMechanicalventilation == "Yes") """)

# COMMAND ----------

#HES CC NIV
#Admissions where BRESSUPDAYS > 0 (i.e there was some BASIC respiratory support)
#ID is in HES_CC AND has U071 or U072 from HES_APC 
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}cc_niv_covid as
SELECT person_id_deid, date,
'03_NIV_treatment' as covid_phenotype,
"" as clinical_code,
"bressupdays > 0" as description,
"confirmed" as covid_status,
"" as code,
'HES CC' as source, date_is, BRESSUPDAYS, ARESSUPDAYS
FROM global_temp.{project_prefix}cc_covid
WHERE BRESSUPDAYS > 0 """)

# COMMAND ----------

#HES APC NIV
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}apc_niv_covid as
SELECT person_id_deid, date, 
"03_NIV_treatment" as covid_phenotype,
(case when OPERTN_4_CONCAT LIKE "%E852%" THEN 'E85.2'
when OPERTN_4_CONCAT LIKE "%E856%" THEN 'E85.6' Else '0' End) as clinical_code,
(case when OPERTN_4_CONCAT LIKE "%E852%" THEN 'Non-invasive ventilation NEC'
when OPERTN_4_CONCAT LIKE "%E856%" THEN 'Continuous positive airway pressure' Else '0' End) as description,
"confirmed" as covid_status,
"HES APC" as source,
"OPCS" as code, date_is, SUSRECID
FROM {collab_database_name}.{project_prefix}{temp_hes_apc}
WHERE (DIAG_4_CONCAT LIKE "%U071%"
   OR DIAG_4_CONCAT LIKE "%U072%")
AND (OPERTN_4_CONCAT LIKE '%E852%' 
      OR OPERTN_4_CONCAT LIKE '%E856%') """)

# COMMAND ----------

#SUS NIV
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}sus_niv_covid as 
SELECT person_id_deid, PRIMARY_PROCEDURE_DATE as date, PROCEDURE_CONCAT,
"03_NIV_treatment" as covid_phenotype,
(case when PROCEDURE_CONCAT LIKE "%E852%" OR PROCEDURE_CONCAT LIKE "%E85.2%" THEN 'E85.2'
when PROCEDURE_CONCAT LIKE "%E856%" OR PROCEDURE_CONCAT LIKE "%E85.6%" THEN 'E85.6' Else '0' End) as clinical_code,
(case when PROCEDURE_CONCAT LIKE "%E852%" OR PROCEDURE_CONCAT LIKE "%E85.2%" THEN 'Non-invasive ventilation NEC'
when PROCEDURE_CONCAT LIKE "%E856%" OR PROCEDURE_CONCAT LIKE "%E85.6%" THEN 'Continuous positive airway pressure' Else '0' End) as description,
"confirmed" as covid_status,
"SUS" as source, 
"OPCS" as code, "PRIMARY_PROCEDURE_DATE" as date_is
FROM {collab_database_name}.{project_prefix}{temp_sus}
WHERE (DIAG_CONCAT LIKE "%U071%"
   OR DIAG_CONCAT LIKE "%U072%") AND
   (PROCEDURE_CONCAT LIKE "%E852%" OR PROCEDURE_CONCAT LIKE "%E85.2%" OR PROCEDURE_CONCAT LIKE "%E856%" OR PROCEDURE_CONCAT LIKE "%E85.6%") AND
   PRIMARY_PROCEDURE_DATE IS NOT NULL """)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.3.3 IMV

# COMMAND ----------

#HES CC IMV
#Admissions where ARESSUPDAYS > 0 (i.e there was some ADVANCED respiratory support)
#ID is in HES_CC AND has U071 or U072 from HES_APC 
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}cc_imv_covid as
SELECT person_id_deid, date,
'03_IMV_treatment' as covid_phenotype,
"" as clinical_code,
"ARESSUPDAYS > 0" as description,
"confirmed" as covid_status,
"" as code,
'HES CC' as source, date_is, BRESSUPDAYS, ARESSUPDAYS
FROM global_temp.{project_prefix}cc_covid
WHERE ARESSUPDAYS > 0 """)

# COMMAND ----------

#CHESS - IMV
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}chess_covid_imv as
SELECT person_id_deid, DateAdmittedICU as date,
"03_IMV_treatment" as covid_phenotype,
"" as clinical_code, 
"Invasivemechanicalventilation == Yes" as description,
"confirmed" as covid_status,
"CHESS" as source, 
"" as code,
"DateAdmittedICU" as date_is -- Using ICU date as probably most of the IMV happened there, but may lose some records (250/10k)
FROM {collab_database_name}.{project_prefix}{temp_chess}
WHERE DateAdmittedICU IS NOT null
AND Invasivemechanicalventilation == "Yes" """)


# COMMAND ----------

#HES APC IMV
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}apc_imv_covid as
SELECT person_id_deid, date, 
"03_IMV_treatment" as covid_phenotype,
(case when OPERTN_4_CONCAT LIKE "%E851%" THEN 'E85.1'
when OPERTN_4_CONCAT LIKE "%X56%" THEN 'X56' Else '0' End) as clinical_code,
(case when OPERTN_4_CONCAT LIKE "%E851%" THEN 'Invasive ventilation'
when OPERTN_4_CONCAT LIKE "%X56%" THEN 'Intubation of trachea' Else '0' End) as description,
"confirmed" as covid_status,
"HES APC" as source, 
"OPCS" as code, date_is, SUSRECID
FROM {collab_database_name}.{project_prefix}{temp_hes_apc}
WHERE (OPERTN_4_CONCAT LIKE '%E851%' 
      OR OPERTN_4_CONCAT LIKE '%X56%') """)

# COMMAND ----------

#SUS IMV
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}sus_imv_covid as 
SELECT person_id_deid, PRIMARY_PROCEDURE_DATE as date, PROCEDURE_CONCAT,
"03_IMV_treatment" as covid_phenotype,
(case when PROCEDURE_CONCAT LIKE "%E851%" OR PROCEDURE_CONCAT LIKE "%E85.1%" THEN 'E85.1'
when PROCEDURE_CONCAT LIKE "%X56%" THEN 'X56' Else '0' End) as clinical_code,
(case when PROCEDURE_CONCAT LIKE "%E851%" OR PROCEDURE_CONCAT LIKE "%E85.1%" THEN 'Invasive ventilation'
when PROCEDURE_CONCAT LIKE "%X56%" THEN 'Intubation of trachea' Else '0' End) as description,
"confirmed" as covid_status,
"SUS" as source, 
"OPCS" as code, "PRIMARY_PROCEDURE_DATE" as date_is
FROM {collab_database_name}.{project_prefix}{temp_sus}
WHERE (DIAG_CONCAT LIKE "%U071%"
   OR DIAG_CONCAT LIKE "%U072%") AND
   (PROCEDURE_CONCAT LIKE "%E851%" OR PROCEDURE_CONCAT LIKE "%E85.1%" OR PROCEDURE_CONCAT LIKE "%X56%") AND
   PRIMARY_PROCEDURE_DATE IS NOT NULL """)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.3.4 EMCO

# COMMAND ----------

#CHESS - ECMO
spark.sql(F"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}chess_covid_ecmo as
SELECT person_id_deid, DateAdmittedICU as date,
"03_ECMO_treatment" as covid_phenotype,
"" as clinical_code, 
"RespiratorySupportECMO == Yes" as description,
"confirmed" as covid_status,
"CHESS" as source, 
"" as code,
"DateAdmittedICU" as date_is -- Reasonable
FROM {collab_database_name}.{project_prefix}{temp_chess}
WHERE DateAdmittedICU IS NOT null
AND RespiratorySupportECMO == "Yes" """)

# COMMAND ----------

#HES APC ECMO
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}apc_ecmo_covid as
SELECT person_id_deid, date, 
"03_ECMO_treatment" as covid_phenotype,
"X58.1" as clinical_code,
"Extracorporeal membrane oxygenation" as description,
"confirmed" as covid_status,
"HES APC" as source, 
"OPCS" as code, date_is, SUSRECID
FROM {collab_database_name}.{project_prefix}{temp_hes_apc}
WHERE (DIAG_4_CONCAT LIKE "%U071%"
   OR DIAG_4_CONCAT LIKE "%U072%")
AND OPERTN_4_CONCAT LIKE '%X581%' """)

# COMMAND ----------

#SUS ECMO
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}sus_ecmo_covid as 
SELECT person_id_deid, PRIMARY_PROCEDURE_DATE as date,
"03_ECMO_treatment" as covid_phenotype,
"X58.1" as clinical_code,
"Extracorporeal membrane oxygenation" as description,
"confirmed" as covid_status,
"SUS" as source, 
"OPCS" as code, "PRIMARY_PROCEDURE_DATE" as date_is
FROM {collab_database_name}.{project_prefix}{temp_sus}
WHERE (DIAG_CONCAT LIKE "%U071%"
   OR DIAG_CONCAT LIKE "%U072%") AND
   (PROCEDURE_CONCAT LIKE "%X58.1%" OR PROCEDURE_CONCAT LIKE "%X581%") AND
   PRIMARY_PROCEDURE_DATE IS NOT NULL """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4: Death from COVID

# COMMAND ----------

#Identify all individuals with a covid diagnosis as death cause
#OBS: 280421 - before we got a max date of death by grouping the query on person_id_deid. I cannot get this working after adding the case bit to determine confrimed/suspected
#              so multiple deaths per person might present in table if they exsist in the raw input!
# CT: Re ^ this is fine as we're presenting a view of the data as it stands, including multiple events etc. further filtering will occurr downstream
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}covid_severe_death as
SELECT person_id_deid, death_date as date,
"04_Fatal_with_covid_diagnosis" as covid_phenotype,
'04_fatal' as covid_severity,
(CASE WHEN S_UNDERLYING_COD_ICD10 LIKE "%U071%" THEN 'U071'
 WHEN S_COD_CODE_1 LIKE "%U071%" THEN 'U071'
 WHEN S_COD_CODE_2 LIKE "%U071%" THEN 'U071'
 WHEN S_COD_CODE_3 LIKE "%U071%" THEN 'U071'
 WHEN S_COD_CODE_4 LIKE "%U071%" THEN 'U071'
 WHEN S_COD_CODE_5 LIKE "%U071%" THEN 'U071'
 WHEN S_COD_CODE_6 LIKE "%U071%" THEN 'U071'
 WHEN S_COD_CODE_7 LIKE "%U071%" THEN 'U071'
 WHEN S_COD_CODE_8 LIKE "%U071%" THEN 'U071'
 WHEN S_COD_CODE_9 LIKE "%U071%" THEN 'U071'
 WHEN S_COD_CODE_10 LIKE "%U071%" THEN 'U071'
 WHEN S_COD_CODE_11 LIKE "%U071%" THEN 'U071'
 WHEN S_COD_CODE_12 LIKE "%U071%" THEN 'U071'
 WHEN S_COD_CODE_13 LIKE "%U071%" THEN 'U071'
 WHEN S_COD_CODE_14 LIKE "%U071%" THEN 'U071'
 WHEN S_COD_CODE_15 LIKE "%U071%" THEN 'U071' Else 'U072' End) as clinical_code, 
'' as description, 
'ICD10' as code, 
'deaths' as source, 'REG_DATE_OF_DEATH' as date_is,
(CASE WHEN S_UNDERLYING_COD_ICD10 LIKE "%U071%" THEN 'confirmed'
 WHEN S_COD_CODE_1 LIKE "%U071%" THEN 'confirmed'
 WHEN S_COD_CODE_2 LIKE "%U071%" THEN 'confirmed'
 WHEN S_COD_CODE_3 LIKE "%U071%" THEN 'confirmed'
 WHEN S_COD_CODE_4 LIKE "%U071%" THEN 'confirmed'
 WHEN S_COD_CODE_5 LIKE "%U071%" THEN 'confirmed'
 WHEN S_COD_CODE_6 LIKE "%U071%" THEN 'confirmed'
 WHEN S_COD_CODE_7 LIKE "%U071%" THEN 'confirmed'
 WHEN S_COD_CODE_8 LIKE "%U071%" THEN 'confirmed'
 WHEN S_COD_CODE_9 LIKE "%U071%" THEN 'confirmed'
 WHEN S_COD_CODE_10 LIKE "%U071%" THEN 'confirmed'
 WHEN S_COD_CODE_11 LIKE "%U071%" THEN 'confirmed'
 WHEN S_COD_CODE_12 LIKE "%U071%" THEN 'confirmed'
 WHEN S_COD_CODE_13 LIKE "%U071%" THEN 'confirmed'
 WHEN S_COD_CODE_14 LIKE "%U071%" THEN 'confirmed'
 WHEN S_COD_CODE_15 LIKE "%U071%" THEN 'confirmed' Else 'suspected' End) as covid_status
FROM {collab_database_name}.{project_prefix}{temp_deaths}
WHERE ((S_UNDERLYING_COD_ICD10 LIKE "%U071%") OR (S_UNDERLYING_COD_ICD10 LIKE "%U072%")) OR
     ((S_COD_CODE_1 LIKE "%U071%") OR (S_COD_CODE_1 LIKE "%U072%")) OR
     ((S_COD_CODE_2 LIKE "%U071%") OR (S_COD_CODE_2 LIKE "%U072%")) OR
     ((S_COD_CODE_3 LIKE "%U071%") OR (S_COD_CODE_3 LIKE "%U072%")) OR
     ((S_COD_CODE_4 LIKE "%U071%") OR (S_COD_CODE_4 LIKE "%U072%")) OR
     ((S_COD_CODE_5 LIKE "%U071%") OR (S_COD_CODE_5 LIKE "%U072%")) OR
     ((S_COD_CODE_6 LIKE "%U071%") OR (S_COD_CODE_6 LIKE "%U072%")) OR
     ((S_COD_CODE_7 LIKE "%U071%") OR (S_COD_CODE_7 LIKE "%U072%")) OR
     ((S_COD_CODE_8 LIKE "%U071%") OR (S_COD_CODE_8 LIKE "%U072%")) OR
     ((S_COD_CODE_9 LIKE "%U071%") OR (S_COD_CODE_9 LIKE "%U072%")) OR
     ((S_COD_CODE_10 LIKE "%U071%") OR (S_COD_CODE_10 LIKE "%U072%")) OR
     ((S_COD_CODE_11 LIKE "%U071%") OR (S_COD_CODE_11 LIKE "%U072%")) OR
     ((S_COD_CODE_12 LIKE "%U071%") OR (S_COD_CODE_12 LIKE "%U072%")) OR
     ((S_COD_CODE_13 LIKE "%U071%") OR (S_COD_CODE_13 LIKE "%U072%")) OR
     ((S_COD_CODE_14 LIKE "%U071%") OR (S_COD_CODE_14 LIKE "%U072%")) OR
     ((S_COD_CODE_15 LIKE "%U071%") OR (S_COD_CODE_15 LIKE "%U072%")) """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4.2 Deaths during COVID admission
# MAGIC Based on Sam Hollings comment:
# MAGIC 
# MAGIC > Something I've been thinking about, is whether the HES column discharge method: "DISMETH" = 4 (the code for "died") should also be used to identify dead patients.
# MAGIC Similarly, a DISDEST = 79 (discharge destination not applicable, died or stillborn)  
# MAGIC   
# MAGIC This appears to add ~ 4k deaths that aren't counted as within 28 days of a positive test (presumably most likely at >= day 29) or with COVID-19 on the death certificate.

# COMMAND ----------

#APC inpatient deaths during COVID-19 admission
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}apc_covid_deaths as
SELECT 
  person_id_deid,
  DISDATE as date,
  "04_Covid_inpatient_death" as covid_phenotype,
  "" as clinical_code,
  (case when 
      DISMETH = 4 THEN 'DISMETH = 4 (Died)'
  when 
      DISDEST = 79 THEN 'DISDEST = 79 (Not applicable - PATIENT died or still birth)' 
  Else '0' End) as description,
  (case when 
      DIAG_4_CONCAT LIKE "%U071%" THEN 'confirmed'
  when 
      DIAG_4_CONCAT LIKE "%U072%" THEN 'suspected' 
  Else '0' End) as covid_status,
  "" as code,
  "HES APC" as source, 
  "DISDATE" as date_is
FROM
  {collab_database_name}.{project_prefix}{temp_hes_apc}
WHERE 
  (DIAG_4_CONCAT LIKE "%U071%" OR DIAG_4_CONCAT LIKE "%U072%")
AND (DISMETH = 4 -- died
      OR 
    DISDEST = 79) -- discharge destination not applicable, died or stillborn
AND (DISDATE >= TO_DATE("20200123", "yyyyMMdd")) -- death after study start 
""")

# COMMAND ----------

#SUS inpatient deaths during COVID-19 admission
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}sus_covid_deaths as
SELECT 
  person_id_deid,
  END_DATE_HOSPITAL_PROVIDER_SPELL as date,
  "04_Covid_inpatient_death" as covid_phenotype,
  "" as clinical_code,
  (case when 
      DISCHARGE_METHOD_HOSPITAL_PROVIDER_SPELL = 4 THEN 'DISCHARGE_METHOD_HOSPITAL_PROVIDER_SPELL = 4 (Died)'
  when 
      DISCHARGE_DESTINATION_HOSPITAL_PROVIDER_SPELL = 79 THEN 'DISCHARGE_DESTINATION_HOSPITAL_PROVIDER_SPELL = 79 (Not applicable - PATIENT died or still birth)' 
  Else '0' End) as description,
  (case when 
      DIAG_CONCAT LIKE "%U071%" THEN 'confirmed'
  when 
      DIAG_CONCAT LIKE "%U072%" THEN 'suspected' 
  Else '0' End) as covid_status,
  "" as code,
  "SUS" as source, 
  "END_DATE_HOSPITAL_PROVIDER_SPELL" as date_is
FROM
  {collab_database_name}.{project_prefix}{temp_sus}
WHERE 
  (DIAG_CONCAT LIKE "%U071%" OR DIAG_CONCAT LIKE "%U072%")
AND (DISCHARGE_METHOD_HOSPITAL_PROVIDER_SPELL = 4 -- died
      OR 
    DISCHARGE_DESTINATION_HOSPITAL_PROVIDER_SPELL = 79) -- discharge destination not applicable, died or stillborn
AND (END_DATE_HOSPITAL_PROVIDER_SPELL IS NOT NULL) """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Trajectory table
# MAGIC Build the trajectory table from all the individual tables created in step 2. This will give a table of IDs, dates and phenotypes which are *not* exclusive. This way we can plot distributions of time intervals between events (test-admission-icu etc.) and take a **data-driven** approach to choosing thresholds to define *exclusive* severity phenotypes.

# COMMAND ----------

spark.sql(F""" drop table if exists {collab_database_name}.{project_prefix}{temp_trajectory}""")

# COMMAND ----------

#Initiate a temporary trajecotry table with the SGSS data
spark.sql(f"""
CREATE TABLE {collab_database_name}.{project_prefix}{temp_trajectory} USING DELTA
as 
SELECT DISTINCT * FROM global_temp.{project_prefix}sgss_covid """)

# COMMAND ----------

spark.sql(f"""
ALTER TABLE {collab_database_name}.{project_prefix}{temp_trajectory} OWNER TO {collab_database_name} """)

# COMMAND ----------

spark.sql(f"""
REFRESH TABLE {collab_database_name}.{project_prefix}{temp_trajectory} """)

# COMMAND ----------

# Append each of the covid related events tables to the temporary trajectory table
for table in ['ccu035_01_gdppr_covid','ccu035_01_apc_covid_any_position','ccu035_01_apc_covid_primary_position','ccu035_01_sus_covid_any_position','ccu035_01_sus_covid_primary_position','ccu035_01_chess_covid_hospital','ccu035_01_chess_covid_icu','ccu035_01_chess_covid_niv','ccu035_01_chess_covid_imv','ccu035_01_chess_covid_ecmo','ccu035_01_cc_covid','ccu035_01_cc_niv_covid','ccu035_01_cc_imv_covid','ccu035_01_apc_niv_covid','ccu035_01_apc_imv_covid','ccu035_01_apc_ecmo_covid', 'ccu035_01_sus_niv_covid', 'ccu035_01_sus_imv_covid', 'ccu035_01_sus_ecmo_covid','ccu035_01_apc_covid_deaths','ccu035_01_sus_covid_deaths']:
  spark.sql(f"""REFRESH TABLE global_temp.{table}""")
  (spark.table(f'global_temp.{table}')
   .select("person_id_deid", "date", "covid_phenotype", "clinical_code", "description", "covid_status", "code", "source", "date_is")
   .distinct()
   .write.format("delta").mode('append')
   .saveAsTable(f"{collab_database_name}.{project_prefix}{temp_trajectory}"))

# COMMAND ----------

## Add on the fatal covid cases with diagnosis
(spark.table(f"global_temp.{project_prefix}covid_severe_death").select("person_id_deid", "date", "covid_phenotype", "clinical_code", "description", "covid_status", "code", "source", "date_is")
    .distinct()
    .write.format("delta").mode('append')
    .saveAsTable(f"{collab_database_name}.{project_prefix}{temp_trajectory}"))

# COMMAND ----------

spark.sql(f"""
OPTIMIZE {collab_database_name}.{project_prefix}{temp_trajectory} ZORDER BY person_id_deid """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Identifying deaths within 28 days

# COMMAND ----------

# Identifying fatal events happing within 28 days of first covid diagnosis/event. 
from pyspark.sql.functions import *

# Get all deaths
all_fatal = spark.sql(f"""SELECT * FROM {collab_database_name}.{project_prefix}{temp_deaths}""")

# Identify earliest non fatal event from trajectory table
first_non_fatal = spark.sql(f"""
WITH list_patients_to_omit AS (SELECT person_id_deid FROM {collab_database_name}.{project_prefix}{temp_trajectory} WHERE covid_phenotype = '04_Fatal_with_covid_diagnosis')
SELECT /*+ BROADCAST(list_patients_to_omit) */
t.person_id_deid, MIN(t.date) AS first_covid_event
FROM {collab_database_name}.{project_prefix}{temp_trajectory} as t
LEFT ANTI JOIN list_patients_to_omit ON t.PERSON_ID_DEID = list_patients_to_omit.PERSON_ID_DEID
GROUP BY t.person_id_deid
""")

# Join with death data - at this stage, not filtering by death cause
# since events with covid as the cause are already in the 
# trajectories table and are excluded in the step above.
first_non_fatal = first_non_fatal.join(all_fatal, ['person_id_deid'], how='left')
first_non_fatal = first_non_fatal.select(['person_id_deid', 'first_covid_event', 'death_date']) 

# Calculate elapsed number of days between earliest event and death date
first_non_fatal = first_non_fatal.withColumn('days_to_death', \
  when(~first_non_fatal['death_date'].isNull(), \
       datediff(first_non_fatal["death_date"], first_non_fatal['first_covid_event'])).otherwise(-1))
 
# Mark deaths within 28 days
first_non_fatal = first_non_fatal.withColumn('28d_death', \
  when((first_non_fatal['days_to_death'] >= 0) & (first_non_fatal['days_to_death'] <= 28), 1).otherwise(0))

# Merge data into main trajectory table (flag as suspected not confirmed!)
first_non_fatal.createOrReplaceGlobalTempView('first_non_fatal')
  

# COMMAND ----------

#Write events to trajectory table
#person_id_deid, date, covid_phenotype, clinical_code, description, covid_status, code, source, date_is
spark.sql(f"""
create or replace global temp view {project_prefix}covid_trajectory AS
select * from {collab_database_name}.{project_prefix}{temp_trajectory} UNION ALL
select distinct
  person_id_deid, 
  death_date as date,
  '04_Fatal_without_covid_diagnosis' as covid_phenotype,
  '' AS clinical_code,
  'ONS death within 28 days' AS description,
  'suspected' AS covid_status,
  '' AS code,
  'deaths' AS source,
  'death_date' AS date_is
FROM
  global_temp.first_non_fatal
WHERE
  28d_death = 1 """)
  

# COMMAND ----------

spark.sql(f""" drop table if exists {collab_database_name}.{project_prefix}{trajectory_final}""")
spark.sql(f""" create table {collab_database_name}.{project_prefix}{trajectory_final} as SELECT * FROM global_temp.{project_prefix}covid_trajectory """)


# COMMAND ----------

spark.sql(f"""
ALTER TABLE {collab_database_name}.{project_prefix}{trajectory_final} OWNER TO {collab_database_name} """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Check counts

# COMMAND ----------

#display(spark.sql(f"""
#SELECT covid_phenotype, source, count(DISTINCT person_id_deid) as count
#FROM {collab_database_name}.{project_prefix}{trajectory_final}
#group by covid_phenotype, source
#order by covid_phenotype """))

# COMMAND ----------

#display(spark.sql(F"""
#SELECT count(DISTINCT person_id_deid)
#FROM {collab_database_name}.{project_prefix}{trajectory_final} """))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Severity table  
# MAGIC Classify all participants according to their most severe COVID-19 event into a severity phenotype (mild, moderate, severe, death), i.e. these are mutually exclusive and patients can only have one severity classification.

# COMMAND ----------

# MAGIC %sql
# MAGIC --All patients exposed to COVID
# MAGIC create or replace global temporary view ccu035_01_covid19_confirmed as
# MAGIC select *
# MAGIC from dars_nic_391419_j3w9t_collab.ccu035_01_covid_trajectory
# MAGIC where covid_status = 'confirmed'
# MAGIC AND (covid_phenotype = '01_Covid_positive_test' OR covid_phenotype = '01_GP_covid_diagnosis' OR covid_phenotype = '02_Covid_admission_any_position' OR covid_phenotype = '02_Covid_admission_primary_position')
# MAGIC AND (source = 'SGSS' OR source = 'HES APC' OR source = 'SUS' OR source = 'GDPPR')

# COMMAND ----------

# MAGIC %sql
# MAGIC --Days between first COVID event and first admission with COVID in primary position
# MAGIC create or replace global temporary view ccu035_01_days_to_covid_admission as
# MAGIC with min_date_covid as(
# MAGIC select person_id_deid, min(date) as first_covid_event
# MAGIC from global_temp.ccu035_01_covid19_confirmed
# MAGIC group by person_id_deid
# MAGIC ),
# MAGIC 
# MAGIC min_date_admission as (
# MAGIC select person_id_deid, min(date) as first_admission_date
# MAGIC from global_temp.ccu035_01_covid19_confirmed
# MAGIC where covid_phenotype = '02_Covid_admission_primary_position'
# MAGIC group by person_id_deid
# MAGIC ),
# MAGIC 
# MAGIC min_date_covid_admission as (
# MAGIC select t1.person_id_deid, t2.first_covid_event, t1.first_admission_date
# MAGIC from min_date_admission t1
# MAGIC left join min_date_covid t2
# MAGIC on t1.person_id_deid = t2.person_id_deid)
# MAGIC 
# MAGIC select *, datediff(first_admission_date,first_covid_event) as days_between
# MAGIC from min_date_covid_admission

# COMMAND ----------

# MAGIC %sql
# MAGIC --Only want patients who were admitted within 28 days
# MAGIC create or replace global temp view ccu035_01_covid_admission_primary as 
# MAGIC select distinct person_id_deid, 'hospitalised' as covid_hospitalisation
# MAGIC from global_temp.ccu035_01_days_to_covid_admission
# MAGIC where days_between <= '28'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Labelling as hospitalised and non-hospitalsied
# MAGIC create or replace global temp view ccu035_01_covid_hospitalised as 
# MAGIC with cte_hospitalised as (
# MAGIC select t1.*, t2.covid_hospitalisation
# MAGIC from global_temp.ccu035_01_covid19_confirmed t1
# MAGIC left join global_temp.ccu035_01_covid_admission_primary t2
# MAGIC on t1.person_id_deid = t2.person_id_deid)
# MAGIC 
# MAGIC select *,
# MAGIC case when covid_hospitalisation IS NULL THEN 'non-hospitalised' ELSE covid_hospitalisation END AS covid_hospitalisation_phenotype
# MAGIC from cte_hospitalised

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Finding first COVID event date
# MAGIC create or replace global temp view ccu035_01_covid_cohort as
# MAGIC select person_id_deid, min(date) AS covid19_confirmed_date, covid_hospitalisation_phenotype
# MAGIC from global_temp.ccu035_01_covid_hospitalised
# MAGIC group by person_id_deid, covid_hospitalisation_phenotype

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1: Mild COVID
# MAGIC * No hosptitalisation
# MAGIC * No death within 4 weeks of first diagnosis
# MAGIC * No death ever with COVID diagnosis

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_covid_mild as
# MAGIC WITH list_patients_to_omit AS (SELECT person_id_deid from dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory WHERE covid_phenotype IN ('02_Covid_admission', '03_ECMO_treatment', '03_IMV_treatment', '03_NIV_treatment', '03_ICU_treatment', '04_Fatal_with_covid_diagnosis','04_Fatal_without_covid_diagnosis', '04_Covid_inpatient_death'))
# MAGIC SELECT /*+ BROADCAST(list_patients_to_omit) */ person_id_deid, min(date) as date, '01_not_hospitalised' as covid_severity 
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory as t
# MAGIC LEFT ANTI JOIN list_patients_to_omit ON t.person_id_deid = list_patients_to_omit.person_id_deid
# MAGIC group by person_id_deid

# COMMAND ----------

# This takes ages 4h 
#drop_table("ccu013_covid_mild")
#create_table("ccu013_covid_mild") 

# COMMAND ----------

# MAGIC %sql
# MAGIC --OPTIMIZE dars_nic_391419_j3w9t_collab.ccu013_covid_mild ZORDER BY person_id_deid

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2: Moderate COVID - Hospitalised
# MAGIC - Hospital admission
# MAGIC - No critical care within that admission
# MAGIC - No death within 4 weeks
# MAGIC - No death from COVID diagnosis ever

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Moderate COVID
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_covid_moderate as
# MAGIC WITH list_patients_to_omit AS (SELECT person_id_deid from dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory WHERE covid_phenotype IN ('03_ECMO_treatment', '03_IMV_treatment', '03_NIV_treatment', '03_ICU_treatment','04_Fatal_with_covid_diagnosis','04_Fatal_without_covid_diagnosis', '04_Covid_inpatient_death'))
# MAGIC SELECT person_id_deid, min(date) as date, '02_hospitalised' as covid_severity 
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory as t
# MAGIC LEFT ANTI JOIN list_patients_to_omit ON t.person_id_deid = list_patients_to_omit.person_id_deid
# MAGIC WHERE (covid_phenotype IN ('02_Covid_admission'))
# MAGIC group by person_id_deid

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 4.3: Severe COVID - Hospitalised with critical care
# MAGIC Hospitalised with one of the following treatments
# MAGIC - ICU treatment 
# MAGIC - NIV and/or IMV treatment
# MAGIC - ECMO treatment

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Severe COVID
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_covid_severe as
# MAGIC WITH list_patients_to_omit AS (SELECT person_id_deid from dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory
# MAGIC WHERE covid_phenotype IN ('04_Fatal_with_covid_diagnosis','04_Fatal_without_covid_diagnosis', '04_Covid_inpatient_death'))
# MAGIC SELECT person_id_deid, min(date) as date, '03_hospitalised_ventilatory_support' as covid_severity 
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory AS t
# MAGIC LEFT ANTI JOIN list_patients_to_omit ON t.person_id_deid = list_patients_to_omit.person_id_deid
# MAGIC WHERE covid_phenotype IN ('03_ECMO_treatment', '03_IMV_treatment', '03_NIV_treatment', '03_ICU_treatment')
# MAGIC group by person_id_deid

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4: Fatal COVID
# MAGIC - Fatal Covid with confirmed or supsected Covid on death register
# MAGIC - Death within 28 days without Covid on death register

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Fatal COVID
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_covid_fatal as
# MAGIC SELECT person_id_deid, min(date) as date, '04_fatal' as covid_severity 
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory
# MAGIC WHERE (covid_phenotype IN ('04_Fatal_with_covid_diagnosis','04_Fatal_without_covid_diagnosis', '04_Covid_inpatient_death'))
# MAGIC group by person_id_deid

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.5: Final combined severity table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu013_covid_severity
# MAGIC as SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_covid_mild
# MAGIC UNION ALL
# MAGIC SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_covid_moderate
# MAGIC UNION ALL
# MAGIC SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_covid_severe
# MAGIC UNION ALL
# MAGIC SELECT * FROM global_temp.ccu013_covid_fatal

# COMMAND ----------

#drop_table("ccu013_covid_severity")
#create_table("ccu013_covid_severity")

# COMMAND ----------

# MAGIC %sql
# MAGIC --OPTIMIZE dars_nic_391419_j3w9t_collab.ccu013_covid_severity

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.6 Check counts

# COMMAND ----------

#%sql
#SELECT covid_severity, count(DISTINCT person_id_deid)
#FROM dars_nic_391419_j3w9t_collab.ccu013_covid_severity
#group by covid_severity
#order by covid_severity

# COMMAND ----------

#%sql
#SELECT count(DISTINCT person_id_deid)
#FROM dars_nic_391419_j3w9t_collab.ccu013_covid_severity

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Step 5: Generate one line per patient and covid index date - extra HW

# COMMAND ----------

# MAGIC %sql
# MAGIC --All patients exposed to COVID
# MAGIC create or replace global temporary view ccu035_01_covid19_confirmed as
# MAGIC select *
# MAGIC from dars_nic_391419_j3w9t_collab.ccu035_01_covid_trajectory
# MAGIC where covid_status = 'confirmed'
# MAGIC AND (covid_phenotype = '01_Covid_positive_test' OR covid_phenotype = '01_GP_covid_diagnosis' OR covid_phenotype = '02_Covid_admission_any_position' OR covid_phenotype = '02_Covid_admission_primary_position')
# MAGIC AND (source = 'SGSS' OR source = 'HES APC' OR source = 'SUS' OR source = 'GDPPR')
# MAGIC AND (date >= '2020-01-01' and date < '2021-12-01' )

# COMMAND ----------

# MAGIC %sql
# MAGIC --Days between first COVID event and first admission to hospital with COVID in primary position
# MAGIC create or replace global temporary view ccu035_01_days_to_covid_admission as
# MAGIC with min_date_covid as(
# MAGIC select person_id_deid, min(date) as first_covid_event
# MAGIC from global_temp.ccu035_01_covid19_confirmed
# MAGIC group by person_id_deid
# MAGIC ),
# MAGIC 
# MAGIC min_date_admission as (
# MAGIC select person_id_deid, min(date) as first_admission_date
# MAGIC from global_temp.ccu035_01_covid19_confirmed
# MAGIC where covid_phenotype = '02_Covid_admission_primary_position'
# MAGIC group by person_id_deid
# MAGIC ),
# MAGIC 
# MAGIC min_date_covid_admission as (
# MAGIC select t1.person_id_deid, t2.first_covid_event, t1.first_admission_date
# MAGIC from min_date_admission t1
# MAGIC left join min_date_covid t2
# MAGIC on t1.person_id_deid = t2.person_id_deid)
# MAGIC 
# MAGIC select *, datediff(first_admission_date,first_covid_event) as days_between
# MAGIC from min_date_covid_admission

# COMMAND ----------

# MAGIC %sql
# MAGIC --Only want patients who were admitted within 28 days
# MAGIC create or replace global temp view ccu035_01_covid_admission_primary as 
# MAGIC select distinct person_id_deid, 'hospitalised' as covid_hospitalisation
# MAGIC from global_temp.ccu035_01_days_to_covid_admission
# MAGIC where days_between <= '28'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Labelling as hospitalised and non-hospitalsied
# MAGIC create or replace global temp view ccu035_01_covid_hospitalised as 
# MAGIC with cte_hospitalised as (
# MAGIC select t1.*, t2.covid_hospitalisation
# MAGIC from global_temp.ccu035_01_covid19_confirmed t1
# MAGIC left join global_temp.ccu035_01_covid_admission_primary t2
# MAGIC on t1.person_id_deid = t2.person_id_deid)
# MAGIC 
# MAGIC select *,
# MAGIC case when covid_hospitalisation IS NULL THEN 'non_hospitalised' ELSE covid_hospitalisation END AS covid_hospitalisation_phenotype
# MAGIC from cte_hospitalised

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Finding first COVID event date
# MAGIC create or replace global temp view ccu035_01_covid_cohort as
# MAGIC select person_id_deid, min(date) AS covid19_confirmed_date, covid_hospitalisation_phenotype
# MAGIC from global_temp.ccu035_01_covid_hospitalised
# MAGIC group by person_id_deid, covid_hospitalisation_phenotype

# COMMAND ----------

# spark.sql(f"""
# create or replace global temp view {project_prefix}covid_index_date AS
# select * from global_temp.ccu035_01_covid_cohort 
# UNION ALL
# select distinct
#   person_id_deid, 
#   covid19_confirmed_date as covid_index_date,
#   covid_hospitalisation_phenotype as covid_phenotype 
# FROM
#   global_temp.ccu035_01_covid_cohort """)

# COMMAND ----------

# create_table(project_prefix + "covid_index_date", select_sql_script=f"SELECT * FROM global_temp.{project_prefix}covid_index_date") 

# COMMAND ----------

tmp = spark.table(f'global_temp.{project_prefix}covid_cohort')
outName = f'{project_prefix}covid_index_date'.lower()  
spark.sql(f'DROP TABLE IF EXISTS {collab_database_name}.{outName}')
tmp.write.mode('overwrite').saveAsTable(f'{collab_database_name}.{outName}')
spark.sql(f'ALTER TABLE {collab_database_name}.{outName} OWNER TO {collab_database_name}')

# COMMAND ----------

tmp1 = tmp\
  .orderBy('person_id_deid')
display(tmp1)

# COMMAND ----------

print(f'{tmp.count():,}')

# COMMAND ----------

#tmp1 = tmp\
#  .select('person_id_deid')\
#  .distinct()
#print(f'{tmp1.count():,}')

# COMMAND ----------

#tmp1 = spark.table(f'{collab_database_name}.ccu035_01_covid_index_date')
#count_var(tmp1, 'person_id_deid')

# COMMAND ----------

#tmp2 = spark.table(f'global_temp.ccu035_01_covid_cohort')
#count_var(tmp2, 'person_id_deid')

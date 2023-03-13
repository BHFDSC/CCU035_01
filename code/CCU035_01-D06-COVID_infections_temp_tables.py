# Databricks notebook source
# MAGIC %md
# MAGIC ## CCU035_01-D07-COVID_infections_temp_tables
# MAGIC  
# MAGIC **Description** 
# MAGIC   
# MAGIC This notebook creates tables for each of the main datasets required to create the COVID infections table. This is based off work from CCU013.
# MAGIC 
# MAGIC **Project(s)** CCU035_01 (from CCU0013 notebook)
# MAGIC  
# MAGIC **Author(s)** Chris Tomlinson, Johan Thygesen (inspired by Sam Hollings!)
# MAGIC  
# MAGIC **Reviewer(s)** 
# MAGIC  
# MAGIC **Date last updated** 23/02/2022 by Hannah Whittaker
# MAGIC  
# MAGIC **Date last reviewed** 
# MAGIC  
# MAGIC **Date last run** <NA>
# MAGIC  
# MAGIC **Data input** 
# MAGIC These are frozen datasets which were frozen in the notebook `CCU035_01-D00-project_table_freeze`
# MAGIC * `dars_nic_391419_j3w9t_collab.ccu035_01_chess_dars_nic_391419_j3w9t`
# MAGIC * `dars_nic_391419_j3w9t_collab.ccu035_01_deaths_dars_nic_391419_j3w9t`
# MAGIC * `dars_nic_391419_j3w9t_collab.ccu035_01_gdppr_dars_nic_391419_j3w9t`
# MAGIC * `dars_nic_391419_j3w9t_collab.ccu035_01_sgss_dars_nic_391419_j3w9t`
# MAGIC * `dars_nic_391419_j3w9t_collab.ccu035_01_sus_dars_nic_391419_j3w9t`
# MAGIC * `dars_nic_391419_j3w9t_collab.ccu035_01_hes_apc_all_years`
# MAGIC * `dars_nic_391419_j3w9t_collab.ccu035_01_hes_op_all_years`
# MAGIC * `dars_nic_391419_j3w9t_collab.ccu035_01_hes_ae_all_years`
# MAGIC * `dars_nic_391419_j3w9t_collab.ccu035_01_hes_cc_all_years`
# MAGIC   
# MAGIC   
# MAGIC **Data output**
# MAGIC * `ccu035_01_tmp_sgss`
# MAGIC * `ccu035_01_tmp_gdppr`
# MAGIC * `ccu035_01_tmp_deaths`
# MAGIC * `ccu035_01_tmp_sus`
# MAGIC * `ccu035_01_tmp_apc`
# MAGIC * `ccu035_01_tmp_cc`
# MAGIC * `ccu035_01_tmp_chess`
# MAGIC   
# MAGIC  
# MAGIC *Previously* these were `GLOBAL TEMP VIEW`, and therefore called in SQL using `FROM global_temp.ccu035_01_X`, however due to lazy evaluation this results in a massive amount of computation at the final point of joining, to distribute this we instead make 'proper' tables named `ccu035_01_tmp_X` and then can delete them after the joining process has occurred.  
# MAGIC   
# MAGIC Saving as a proper table also allows us to run optimisation to improve speed of future queries & joins.
# MAGIC 
# MAGIC **Software and versions** SQl, Python
# MAGIC  
# MAGIC **Packages and versions** `pyspark`

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

#Date parameters
index_date = '2020-01-01' 

#Output
temp_sgss = 'tmp_sgss'
temp_gdppr = 'tmp_gdppr'
temp_deaths = 'tmp_deaths'
temp_hes_apc = 'tmp_hes_apc'
temp_hes_cc = 'tmp_hes_cc'
temp_chess = 'tmp_chess'
temp_sus = 'tmp_sus'

# COMMAND ----------

from pyspark.sql.functions import lit, to_date, col, udf, substring
from datetime import datetime, date #Jenny added date
from pyspark.sql.types import DateType


today = date.today()
end_date = today.strftime('%Y-%m-%d') #Jenny changed this to todays date


# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU035_01/CCU035_01-functions/CCU035_01_COVID_trajectory_helper_functions_from_CCU013

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.0 Subseting all source tables by dates

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 SGSS

# COMMAND ----------

# SGSS
sgss = spark.sql(f"""SELECT person_id_deid, REPORTING_LAB_ID, specimen_date FROM {collab_database_name}.{project_prefix}{sgss_data}""")
sgss = sgss.withColumnRenamed('specimen_date', 'date')
sgss = sgss.withColumn('date_is', lit('specimen_date'))
sgss = sgss.filter((sgss['date'] >= index_date) & (sgss['date'] <= end_date))
sgss = sgss.filter(sgss['person_id_deid'].isNotNull())
sgss.createOrReplaceGlobalTempView(f"{project_prefix}tmp_sgss")
#drop_table(collab_database_name +"." + project_prefix + temp_sgss + temp_current_initials_date, if_exists=True)
create_table(project_prefix + temp_sgss, select_sql_script=f"SELECT * FROM global_temp.{project_prefix}tmp_sgss") 
#sgss = sgss.orderBy('specimen_date', ascending = False)
#display(sgss)
#print(sgss.count(), len(sgss.columns))

# COMMAND ----------

# MAGIC %sql -- create_table makes all tables as deltatables by default optimize
# MAGIC --OPTIMIZE dars_nic_391419_j3w9t_collab.ccu035_01_tmp_sgss

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 GDPPR

# COMMAND ----------

# GDPPR
gdppr = spark.sql(f"""SELECT NHS_NUMBER_DEID, DATE, LSOA, code FROM {collab_database_name}.{project_prefix}{gdppr_data}""")
gdppr = gdppr.withColumnRenamed('DATE', 'date').withColumnRenamed('NHS_NUMBER_DEID', 'person_id_deid')
gdppr = gdppr.withColumn('date_is', lit('DATE'))
gdppr = gdppr.filter((gdppr['date'] >= index_date) & (gdppr['date'] <= end_date))
gdppr.createOrReplaceGlobalTempView(f"{project_prefix}tmp_gdppr")
#display(gdppr)
drop_table(project_prefix + temp_gdppr + temp_current_initials_date, if_exists=True)
create_table(project_prefix + temp_gdppr, select_sql_script=f"SELECT * FROM global_temp.{project_prefix}tmp_gdppr") 

# COMMAND ----------

# MAGIC %sql -- create_table makes all tables as deltatables by default optimize
# MAGIC --OPTIMIZE dars_nic_391419_j3w9t_collab.ccu035_01_tmp_gdppr

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Deaths

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ensure only single death per ID
# MAGIC -- CURENTLY NOT IMPLEMETNED AS THIS CAUSES ISSUES WITH THE DEATH WITHOUT COVID DETECTION!
# MAGIC --CREATE OR REPLACE GLOBAL TEMP VIEW ccu0_dp_single_patient_death AS
# MAGIC --SELECT * 
# MAGIC --FROM 
# MAGIC --  (SELECT * , 
# MAGIC --  to_date(REG_DATE_OF_DEATH, "yyyyMMdd") as REG_DATE_OF_DEATH_formatted,
# MAGIC --  row_number()  OVER (PARTITION BY DEC_CONF_NHS_NUMBER_CLEAN_DEID 
# MAGIC --                                      ORDER BY REG_DATE desc, REG_DATE_OF_DEATH desc) as death_rank
# MAGIC --    FROM dars_nic_391419_j3w9t_collab.ccu003_direfcts_dataprep_1_deaths_frzon28may_mm_210528
# MAGIC --  ) cte
# MAGIC --WHERE death_rank = 1
# MAGIC --AND DEC_CONF_NHS_NUMBER_CLEAN_DEID IS NOT NULL
# MAGIC --AND REG_DATE_OF_DEATH_formatted > '1900-01-01'
# MAGIC --AND REG_DATE_OF_DEATH_formatted <= current_date()

# COMMAND ----------

# Deaths
death = spark.sql(f"""SELECT * FROM {collab_database_name}.{project_prefix}{deaths_data}""")
death = death.withColumn("death_date", to_date(death['REG_DATE_OF_DEATH'], "yyyyMMdd"))
death = death.withColumnRenamed('DEC_CONF_NHS_NUMBER_CLEAN_DEID', 'person_id_deid')
death = death.withColumn('date_is', lit('REG_DATE_OF_DEATH'))
death = death.filter((death['death_date'] >= index_date) & (death['death_date'] <= end_date))
death = death.filter(death['person_id_deid'].isNotNull())
death.createOrReplaceGlobalTempView(f"{project_prefix}tmp_deaths")

drop_table(project_prefix + temp_deaths + temp_current_initials_date, if_exists=True)
create_table(project_prefix + temp_deaths , select_sql_script=f"SELECT * FROM global_temp.{project_prefix}tmp_deaths") 

# COMMAND ----------

# MAGIC %sql -- create_table makes all tables as deltatables by default optimize
# MAGIC --OPTIMIZE dars_nic_391419_j3w9t_collab.ccu035_01_tmp_deaths

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 HES OP - **Not currently in use**

# COMMAND ----------

# HES OP
# Not currently included
#op = spark.sql('''SELECT tab1.* FROM dars_nic_391419_j3w9t_collab.ccu003_direfcts_dataprep_1_op_all_frzon28may_mm_210528 as tab1
#                  INNER JOIN dars_nic_391419_j3w9t_collab.ccu0_total_population ON tab1.PERSON_ID_DEID = ccu0_total_population.person_id_deid''')
#op = op.withColumnRenamed('PERSON_ID_DEID', 'person_id_deid').withColumnRenamed('APPTDATE', 'date')
#op = op.filter(op['person_id_deid'].isNotNull())
#op = op.withColumn('date_is', lit('APPTDATE'))
#op = op.filter((op['date'] >= start_date) & (op['date'] <= end_date))
#op.createOrReplaceGlobalTempView('ccu0_tmp_op')
#drop_table("ccu0_tmp_op")
#create_table("ccu0_tmp_op") 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT max(date) FROM dars_nic_391419_j3w9t_collab.ccu0_tmp_op

# COMMAND ----------

# %sql -- create_table makes all tables as deltatables by default optimize
# OPTIMIZE dars_nic_391419_j3w9t_collab.ccu0_tmp_op

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.5 HES APC

# COMMAND ----------

# HES APC with suspected or confirmed COVID-19
apc = spark.sql(f"""SELECT PERSON_ID_DEID, EPISTART,DIAG_4_01, DIAG_4_CONCAT, OPERTN_4_CONCAT, DISMETH, DISDEST, DISDATE, SUSRECID FROM {collab_database_name}.{project_prefix}{hes_apc_data} WHERE DIAG_4_CONCAT LIKE "%U071%" OR DIAG_4_CONCAT LIKE "%U072%" """)
apc = apc.withColumnRenamed('PERSON_ID_DEID', 'person_id_deid').withColumnRenamed('EPISTART', 'date')
apc = apc.withColumn('date_is', lit('EPISTART'))
apc = apc.filter((apc['date'] >= index_date) & (apc['date'] <= end_date))
apc = apc.filter(apc['person_id_deid'].isNotNull())
apc.createOrReplaceGlobalTempView(f"{project_prefix}tmp_apc")
#display(apc)
drop_table(project_prefix + temp_hes_apc, if_exists=True)
create_table(project_prefix + temp_hes_apc , select_sql_script=f"SELECT * FROM global_temp.{project_prefix}tmp_apc") 

# COMMAND ----------

# MAGIC %sql
# MAGIC --OPTIMIZE dars_nic_391419_j3w9t_collab.ccu035_01_tmp_apc

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.6 HES CC

# COMMAND ----------

# HES CC
cc = spark.sql(f"""SELECT * FROM {collab_database_name}.{project_prefix}{hes_cc_data} """)
cc = cc.withColumnRenamed('CCSTARTDATE', 'date').withColumnRenamed('PERSON_ID_DEID', 'person_id_deid')
cc = cc.withColumn('date_is', lit('CCSTARTDATE'))
# reformat dates for hes_cc as currently strings
asDate = udf(lambda x: datetime.strptime(x, '%Y%m%d'), DateType())
cc = cc.filter(cc['person_id_deid'].isNotNull())
cc = cc.withColumn('date', asDate(col('date')))
cc = cc.filter((cc['date'] >= index_date) & (cc['date'] <= end_date))
cc.createOrReplaceGlobalTempView(f"{project_prefix}tmp_cc")
#display(cc)
drop_table(project_prefix + temp_hes_cc, if_exists=True)
create_table(project_prefix + temp_hes_cc, select_sql_script=f"SELECT * FROM global_temp.{project_prefix}tmp_cc") 

# COMMAND ----------

# MAGIC %sql -- create_table makes all tables as deltatables by default optimize
# MAGIC --OPTIMIZE dars_nic_391419_j3w9t_collab.ccu035_01_tmp_cc

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.7 Pillar 2 Antigen testing **Not currently in use**
# MAGIC Briefly this dataset should be entirely encapsulated within SGSS (which includes Pilars 1 & 2), however this was not the case. We were additionally detecting multiple tests per indidivdual, in contrast to the dataset specification. This dataset has currently been recalled by NHS-Digital.

# COMMAND ----------

# # pillar2 only include positve tests for now!
# pillar2 = spark.sql("""
# SELECT Person_ID_DEID as person_id_deid, 
# AppointmentDate as date, 
# CountryCode,
# TestResult,
# ResultInfo,
# 'AppointmentDate' as date_is,
# 'Pillar 2' as source,
# TestType, TestLocation, AdministrationMethod -- these seem the interesting 3 variables to me
# FROM dars_nic_391419_j3w9t_collab.ccu003_direfcts_dataprep_1_pillar2_frzon28may_mm_210528
# WHERE TestResult IN ('SCT:1322781000000102','SCT:1240581000000104')""")
# # reformat date as currently string
# pillar2 = pillar2.withColumn('date', substring('date', 0, 10)) # NB pillar2 dates in 2019-01-01T00:00:0000. format, therefore subset first
# pillar2 = pillar2.withColumn('TestResult', regexp_replace('TestResult', 'SCT:', ''))
# pillar2 = pillar2.filter(pillar2['person_id_deid'].isNotNull())
# pillar2 = pillar2.filter(pillar2['date'].isNotNull())
# asDate = udf(lambda x: datetime.strptime(x, '%Y-%m-%d'), DateType())
# pillar2 = pillar2.withColumn('date', asDate(col('date')))
# # Trim dates
# pillar2 = pillar2.filter((pillar2['date'] >= start_date) & (pillar2['date'] <= end_date))
# pillar2.createOrReplaceGlobalTempView('ccu0_tmp_pillar2')

# COMMAND ----------

# drop_table("ccu0_tmp_pillar2")
# create_table("ccu0_tmp_pillar2")

# COMMAND ----------

# %sql
# SELECT max(date) FROM dars_nic_391419_j3w9t_collab.ccu0_tmp_pillar2

# COMMAND ----------

# %sql -- create_table makes all tables as deltatables by default optimize
# OPTIMIZE dars_nic_391419_j3w9t_collab.ccu0_tmp_pillar2

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.8 CHESS

# COMMAND ----------

chess = spark.sql(f"""
SELECT PERSON_ID_DEID as person_id_deid,
Typeofspecimen,
Covid19,
AdmittedToICU,
Highflownasaloxygen, 
NoninvasiveMechanicalventilation,
Invasivemechanicalventilation,
RespiratorySupportECMO,
DateAdmittedICU,
HospitalAdmissionDate,
InfectionSwabDate as date, 
'InfectionSwabDate' as date_is
FROM {collab_database_name}.{project_prefix}{chess_data}
""")
chess = chess.filter(chess['Covid19'] == 'Yes')
chess = chess.filter(chess['person_id_deid'].isNotNull())
chess = chess.filter((chess['date'] >= index_date) & (chess['date'] <= end_date))
chess = chess.filter(((chess['date'] >= index_date) | (chess['date'].isNull())) & ((chess['date'] <= end_date) | (chess['date'].isNull())))
chess = chess.filter(((chess['HospitalAdmissionDate'] >= index_date) | (chess['HospitalAdmissionDate'].isNull())) & ((chess['HospitalAdmissionDate'] <= end_date) | (chess['HospitalAdmissionDate'].isNull())))
chess = chess.filter(((chess['DateAdmittedICU'] >= index_date) | (chess['DateAdmittedICU'].isNull())) & ((chess['DateAdmittedICU'] <= end_date) | (chess['DateAdmittedICU'].isNull())))
chess.createOrReplaceGlobalTempView(f"{project_prefix}tmp_chess")
#display(chess)
drop_table(project_prefix + temp_chess + temp_current_initials_date, if_exists=True)
create_table(project_prefix + temp_chess, select_sql_script=f"SELECT * FROM global_temp.{project_prefix}tmp_chess") 

# COMMAND ----------

# MAGIC %sql -- create_table makes all tables as deltatables by default optimize
# MAGIC --OPTIMIZE dars_nic_391419_j3w9t_collab.ccu035_01_tmp_chess

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.9 SUS
# MAGIC NB this is a large dataset and the coalescing of diagnosis & procedure fields into a format compatible with our HES queries takes some time (~40 mins)

# COMMAND ----------

## SUS
# ! Takes ~ 40 mins 
sus = spark.sql(f"""SELECT NHS_NUMBER_DEID, EPISODE_START_DATE, PRIMARY_DIAGNOSIS_CODE,
CONCAT (COALESCE(PRIMARY_DIAGNOSIS_CODE, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_1, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_2, ''), ',',
COALESCE(SECONDARY_DIAGNOSIS_CODE_3, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_4, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_5, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_6, ''), ',',
COALESCE(SECONDARY_DIAGNOSIS_CODE_7, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_8, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_9, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_10, ''), ',',
COALESCE(SECONDARY_DIAGNOSIS_CODE_11, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_12, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_13, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_14, ''), ',',
COALESCE(SECONDARY_DIAGNOSIS_CODE_15, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_16, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_17, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_18, ''), ',',
COALESCE(SECONDARY_DIAGNOSIS_CODE_19, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_20, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_21, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_22, ''), ',',
COALESCE(SECONDARY_DIAGNOSIS_CODE_23, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_24, '')) as DIAG_CONCAT,
CONCAT (COALESCE(PRIMARY_PROCEDURE_CODE, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_1, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_2, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_1, ''),  ',',
COALESCE(SECONDARY_PROCEDURE_CODE_3, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_4, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_5, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_6, ''), ',',
COALESCE(SECONDARY_PROCEDURE_CODE_7, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_8, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_9, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_10, ''), ',',
COALESCE(SECONDARY_PROCEDURE_CODE_11, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_12, '')) as PROCEDURE_CONCAT, PRIMARY_PROCEDURE_DATE, SECONDARY_PROCEDURE_DATE_1,
DISCHARGE_DESTINATION_HOSPITAL_PROVIDER_SPELL, DISCHARGE_METHOD_HOSPITAL_PROVIDER_SPELL, END_DATE_HOSPITAL_PROVIDER_SPELL
FROM {collab_database_name}.{project_prefix}{sus_data}
""")
## Other potential interstersting columns: END_DATE_HOSPITAL_PROVIDER_SPELL, EPISODE_START_DATE, EPISODE_END_DATE, PRIMARY_PROCEDURE_CODE, PRIMARY_PROCEDURE_DATE, SECONDARY_PROCEDURE_CODE1 - 12
sus = sus.withColumnRenamed('NHS_NUMBER_DEID', 'person_id_deid').withColumnRenamed('EPISODE_START_DATE', 'date')
sus = sus.withColumn('date_is', lit('EPISODE_START_DATE'))
sus = sus.filter((sus['date'] >= index_date) & (sus['date'] <= end_date))
sus = sus.filter(((sus['END_DATE_HOSPITAL_PROVIDER_SPELL'] >= index_date) | (sus['END_DATE_HOSPITAL_PROVIDER_SPELL'].isNull())) & 
                     ((sus['END_DATE_HOSPITAL_PROVIDER_SPELL'] <= end_date) | (sus['END_DATE_HOSPITAL_PROVIDER_SPELL'].isNull())))
sus = sus.filter(sus['person_id_deid'].isNotNull()) # Loads of rows with missing IDs
sus = sus.filter(sus['date'].isNotNull())
sus.createOrReplaceGlobalTempView(f"{project_prefix}tmp_sus")
#display(sus)
drop_table(project_prefix + temp_sus + temp_current_initials_date, if_exists=True)
create_table(project_prefix + temp_sus, select_sql_script=f"SELECT * FROM global_temp.{project_prefix}tmp_sus") 

# COMMAND ----------

# MAGIC %sql -- create_table makes all tables as deltatables by default optimize
# MAGIC -- ! Takes ~ 24 mins
# MAGIC --OPTIMIZE dars_nic_391419_j3w9t_collab.ccu035_01_tmp_sus

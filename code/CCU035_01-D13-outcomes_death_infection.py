# Databricks notebook source
# MAGIC %md #Death: CVD outcomes (work package 2.5)
# MAGIC **Description** This notebooks make a table ccu035_death_outcomes which contains all observations with the relevant observations containing outcomes clinical codes from the death dataset recorded after **covid19_confirmed_date**. Phenotypes used are listed below:
# MAGIC 
# MAGIC 
# MAGIC Where multiple enteries for a person, use the most recent death date
# MAGIC 
# MAGIC **Project(s)** CCU035_01
# MAGIC 
# MAGIC **Author(s)** Spencer Keene adapted from Rachel Denholm's notebooks. Updated by Hannah Whittaker for project cuu035_01.
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
# MAGIC **Data output table:** ccu035_death_outcomes
# MAGIC 
# MAGIC **Software and versions** SQL
# MAGIC 
# MAGIC **Packages and versions** 'Not applicable'

# COMMAND ----------

# MAGIC %run "/Workspaces/dars_nic_391419_j3w9t_collab/SHDS/common/functions"

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

# MAGIC %md 
# MAGIC #####Infection parameters

# COMMAND ----------

# MAGIC %py
# MAGIC 
# MAGIC #Dataset parameters
# MAGIC deaths_data = 'dars_nic_391419_j3w9t_collab.ccu035_01_deaths_dars_nic_391419_j3w9t'
# MAGIC 
# MAGIC index_dates='dars_nic_391419_j3w9t_collab.ccu035_01_start_and_end_date'
# MAGIC project_prefix = 'ccu035_01_'
# MAGIC collab_database_name = 'dars_nic_391419_j3w9t_collab'
# MAGIC 
# MAGIC 
# MAGIC #Final table name
# MAGIC final_table = 'ccu035_01_inf_outcomes_deaths_final' 

# COMMAND ----------

# MAGIC %py
# MAGIC 
# MAGIC spark.sql(F"""create or replace global temp view ccu035_01_infection_deaths
# MAGIC as select *
# MAGIC from {deaths_data}""") 

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from global_temp.ccu035_01_infection_deaths

# COMMAND ----------

# MAGIC %sql
# MAGIC ----All HES codelists used
# MAGIC 
# MAGIC create or replace global temp view ccu035_01_infection_deaths_outcomes_map as
# MAGIC 
# MAGIC SELECT LEFT ( REGEXP_REPLACE(code,'[.,-,' ']','') , 4 ) AS ICD10code_trunc,
# MAGIC name, terminology, code, term, code_type, RecordDate
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu035_01_codelist
# MAGIC WHERE (name = 'AMI' OR name = 'HF' OR name = 'angina' OR name = 'stroke_TIA' OR name = 'stroke_isch' OR name = 'unstable_angina' OR name = 'DIC' OR name = 'DVT_DVT' OR name = 'DVT_ICVT' OR name = 'DVT_pregnancy' OR name = 'ICVT_pregnancy' OR name = 'PE' OR name = 'TTP' OR name = 'artery_dissect' OR name = 'cardiomyopathy' OR name = 'fracture' OR name = 'life_arrhythmia' OR name = 'mesenteric_thrombus' OR name = 'myocarditis' OR name = 'other_DVT' OR name = 'other_arterial_embolism' OR name = 'pericarditis' OR name = 'portal_vein_thrombosis' OR name = 'stroke_SAH_HS' OR name = 'thrombocytopenia' OR name = 'thrombophilia')
# MAGIC       AND terminology = 'ICD10' --AND code_type=1 AND RecordDate=20210127
# MAGIC       

# COMMAND ----------

# MAGIC %sql
# MAGIC select name
# MAGIC from global_temp.ccu035_01_infection_deaths_outcomes_map
# MAGIC group by name

# COMMAND ----------

# MAGIC %md 
# MAGIC ###First diagnosis positions

# COMMAND ----------

# MAGIC %sql
# MAGIC ---Using latest death date - latest date of death from latest registered date
# MAGIC ---created truncated caliber codes with dot missing and using all outcomes
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu035_01_infection_death_outcomes_first_position  AS
# MAGIC --create or replace global temp view ccu035_infection_death_outcomes_first_position as 
# MAGIC 
# MAGIC with cte_anydeath as (
# MAGIC SELECT *
# MAGIC FROM
# MAGIC   (select DEC_CONF_NHS_NUMBER_CLEAN_DEID as ID, REG_DATE_OF_DEATH_FORMATTED,
# MAGIC    row_number() OVER(PARTITION BY DEC_CONF_NHS_NUMBER_CLEAN_DEID
# MAGIC                          ORDER BY REG_DATE desc, REG_DATE_OF_DEATH_FORMATTED desc) as death_rank
# MAGIC   FROM global_temp.ccu035_01_infection_deaths
# MAGIC --WHERE REG_DATE_OF_DEATH_FORMATTED > covid19_confirmed_date AND REG_DATE_OF_DEATH_FORMATTED < end_date
# MAGIC --AND REG_DATE_OF_DEATH_FORMATTED <=current_date()
# MAGIC   )
# MAGIC ),
# MAGIC 
# MAGIC cte_anydeath_latest as (
# MAGIC select *
# MAGIC from cte_anydeath where death_rank = 1
# MAGIC ),
# MAGIC 
# MAGIC cte_deathcvd as (
# MAGIC SELECT * 
# MAGIC FROM 
# MAGIC   (SELECT DEC_CONF_NHS_NUMBER_CLEAN_DEID, REG_DATE_OF_DEATH_FORMATTED, S_UNDERLYING_COD_ICD10, S_COD_CODE_1,
# MAGIC   S_COD_CODE_2, S_COD_CODE_3, S_COD_CODE_4, S_COD_CODE_5, S_COD_CODE_6, S_COD_CODE_7, S_COD_CODE_8,
# MAGIC   S_COD_CODE_9, S_COD_CODE_10, S_COD_CODE_11, S_COD_CODE_12, S_COD_CODE_13, S_COD_CODE_14, S_COD_CODE_15,
# MAGIC    LEFT ( REGEXP_REPLACE(S_UNDERLYING_COD_ICD10,'[X]$','') , 3 ) AS S_UNDERLYING_COD_ICD10_trunc,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_1,'[X]$','') , 3 ) AS S_COD_CODE_1_trunc,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_2,'[X]$','') , 3 ) AS S_COD_CODE_2_trunc,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_3,'[X]$','') , 3 ) AS S_COD_CODE_3_trunc,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_4,'[X]$','') , 3 ) AS S_COD_CODE_4_trunc,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_5,'[X]$','') , 3 ) AS S_COD_CODE_5_trunc,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_6,'[X]$','') , 3 ) AS S_COD_CODE_6_trunc,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_7,'[X]$','') , 3 ) AS S_COD_CODE_7_trunc,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_8,'[X]$','') , 3 ) AS S_COD_CODE_8_trunc,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_9,'[X]$','') , 3 ) AS S_COD_CODE_9_trunc,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_10,'[X]$','') , 3 ) AS S_COD_CODE_10_trunc,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_11,'[X]$','') , 3 ) AS S_COD_CODE_11_trunc,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_12,'[X]$','') , 3 ) AS S_COD_CODE_12_trunc,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_13,'[X]$','') , 3 ) AS S_COD_CODE_13_trunc,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_14,'[X]$','') , 3 ) AS S_COD_CODE_14_trunc,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_15,'[X]$','') , 3 ) AS S_COD_CODE_15_trunc,
# MAGIC    LEFT ( REGEXP_REPLACE(S_UNDERLYING_COD_ICD10,'[X]$','') , 4 ) AS S_UNDERLYING_COD_ICD10_trunc_4,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_1,'[X]$','') , 4 ) AS S_COD_CODE_1_trunc_4,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_2,'[X]$','') , 4 ) AS S_COD_CODE_2_trunc_4,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_3,'[X]$','') , 4 ) AS S_COD_CODE_3_trunc_4,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_4,'[X]$','') , 4 ) AS S_COD_CODE_4_trunc_4,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_5,'[X]$','') , 4 ) AS S_COD_CODE_5_trunc_4,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_6,'[X]$','') , 4 ) AS S_COD_CODE_6_trunc_4,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_7,'[X]$','') , 4 ) AS S_COD_CODE_7_trunc_4,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_8,'[X]$','') , 4 ) AS S_COD_CODE_8_trunc_4,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_9,'[X]$','') , 4 ) AS S_COD_CODE_9_trunc_4,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_10,'[X]$','') , 4 ) AS S_COD_CODE_10_trunc_4,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_11,'[X]$','') , 4 ) AS S_COD_CODE_11_trunc_4,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_12,'[X]$','') , 4 ) AS S_COD_CODE_12_trunc_4,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_13,'[X]$','') , 4 ) AS S_COD_CODE_13_trunc_4,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_14,'[X]$','') , 4 ) AS S_COD_CODE_14_trunc_4,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_15,'[X]$','') , 4 ) AS S_COD_CODE_15_trunc_4
# MAGIC   FROM global_temp.ccu035_01_infection_deaths
# MAGIC   --WHERE REG_DATE_OF_DEATH_FORMATTED > covid19_confirmed_date AND REG_DATE_OF_DEATH_FORMATTED < end_date
# MAGIC   )
# MAGIC ),
# MAGIC 
# MAGIC cte_deathcvd_link as (
# MAGIC select *
# MAGIC from cte_deathcvd t1
# MAGIC inner join global_temp.ccu035_01_infection_deaths_outcomes_map t2 on 
# MAGIC (t1.S_UNDERLYING_COD_ICD10_trunc = t2.ICD10code_trunc
# MAGIC /*OR t1.S_COD_CODE_1_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_2_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_3_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_4_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_5_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_6_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_7_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_8_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_9_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_10_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_11_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_12_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_13_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_14_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_15_trunc = t2.ICD10code_trunc*/
# MAGIC OR t1.S_UNDERLYING_COD_ICD10_trunc_4 = t2.ICD10code_trunc
# MAGIC /*OR t1.S_COD_CODE_1_trunc_4 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_2_trunc_4 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_3_trunc_4 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_4_trunc_4 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_5_trunc_4 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_6_trunc_4 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_7_trunc_4 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_8_trunc_4 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_9_trunc_4 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_10_trunc_4 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_11_trunc_4 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_12_trunc_4 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_13_trunc_4 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_14_trunc_4 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_15_trunc_4 = t2.ICD10code_trunc*/
# MAGIC )
# MAGIC )
# MAGIC 
# MAGIC 
# MAGIC --cte_next as (
# MAGIC select tab1.ID, tab1.REG_DATE_OF_DEATH_FORMATTED, tab2.DEC_CONF_NHS_NUMBER_CLEAN_DEID, tab2.name, tab2.term, tab2.ICD10code_trunc as code 
# MAGIC from cte_anydeath_latest tab1
# MAGIC left join cte_deathcvd_link tab2 on tab1.ID = tab2.DEC_CONF_NHS_NUMBER_CLEAN_DEID 
# MAGIC --)
# MAGIC 
# MAGIC --select *
# MAGIC --from cte_next

# COMMAND ----------

index_dates=spark.table (f'dars_nic_391419_j3w9t_collab.ccu035_01_start_and_end_date')

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table  global_temp.ccu035_01_infection_death_outcomes_first_position

# COMMAND ----------

tmp=spark.table(f'global_temp.{project_prefix}infection_death_outcomes_first_position')\
  .withColumnRenamed('ID', 'NHS_NUMBER_DEID')\
  .join(index_dates, on='NHS_NUMBER_DEID', how='inner')\
  .where((f.col('covid19_confirmed_date') < f.col('REG_DATE_OF_DEATH_FORMATTED')) & (f.col('end_date') >= f.col('REG_DATE_OF_DEATH_FORMATTED')))
tmp.createOrReplaceGlobalTempView(f'{project_prefix}infection_death_outcomes_first_position')

# COMMAND ----------

count_var(tmp, 'NHS_NUMBER_DEID')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(Distinct NHS_NUMBER_DEID) as count, name
# MAGIC FROM global_temp.ccu035_01_infection_death_outcomes_first_position
# MAGIC --from global_temp.ccu035_infection_death_outcomes
# MAGIC group by name
# MAGIC order by count desc 

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table  dars_nic_391419_j3w9t_collab.ccu035_01_infection_death_outcomes_first_position

# COMMAND ----------

create_table('ccu035_01_infection_death_outcomes_first_position' , select_sql_script=f"SELECT * FROM global_temp.ccu035_01_infection_death_outcomes_first_position") 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(Distinct NHS_NUMBER_DEID) as count 
# MAGIC FROM dars_nic_391419_j3w9t_collab.CCU035_01_infection_death_outcomes_first_position

# COMMAND ----------

# MAGIC %md 
# MAGIC ###All diagnosis positions

# COMMAND ----------

# MAGIC %sql
# MAGIC ---Using latest death date - latest date of death from latest registered date
# MAGIC ---created truncated caliber codes with dot missing and using all outcomes
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu035_01_infection_death_outcomes  AS
# MAGIC --create or replace global temp view ccu035_infection_death_outcomes as 
# MAGIC 
# MAGIC with cte_anydeath as (
# MAGIC SELECT *
# MAGIC FROM
# MAGIC   (select DEC_CONF_NHS_NUMBER_CLEAN_DEID as ID, REG_DATE_OF_DEATH_FORMATTED,
# MAGIC    row_number() OVER(PARTITION BY DEC_CONF_NHS_NUMBER_CLEAN_DEID
# MAGIC                          ORDER BY REG_DATE desc, REG_DATE_OF_DEATH_FORMATTED desc) as death_rank
# MAGIC   FROM global_temp.ccu035_01_infection_deaths
# MAGIC --WHERE REG_DATE_OF_DEATH_FORMATTED > '2019-12-31' AND REG_DATE_OF_DEATH_FORMATTED < '2020-12-08'
# MAGIC --AND REG_DATE_OF_DEATH_FORMATTED <=current_date()
# MAGIC   )
# MAGIC ),
# MAGIC 
# MAGIC cte_anydeath_latest as (
# MAGIC select *
# MAGIC from cte_anydeath where death_rank = 1
# MAGIC ),
# MAGIC 
# MAGIC cte_deathcvd as (
# MAGIC SELECT * 
# MAGIC FROM 
# MAGIC   (SELECT DEC_CONF_NHS_NUMBER_CLEAN_DEID, REG_DATE_OF_DEATH_FORMATTED, S_UNDERLYING_COD_ICD10, S_COD_CODE_1,
# MAGIC   S_COD_CODE_2, S_COD_CODE_3, S_COD_CODE_4, S_COD_CODE_5, S_COD_CODE_6, S_COD_CODE_7, S_COD_CODE_8,
# MAGIC   S_COD_CODE_9, S_COD_CODE_10, S_COD_CODE_11, S_COD_CODE_12, S_COD_CODE_13, S_COD_CODE_14, S_COD_CODE_15,
# MAGIC    LEFT ( REGEXP_REPLACE(S_UNDERLYING_COD_ICD10,'[X]$','') , 3 ) AS S_UNDERLYING_COD_ICD10_trunc,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_1,'[X]$','') , 3 ) AS S_COD_CODE_1_trunc,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_2,'[X]$','') , 3 ) AS S_COD_CODE_2_trunc,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_3,'[X]$','') , 3 ) AS S_COD_CODE_3_trunc,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_4,'[X]$','') , 3 ) AS S_COD_CODE_4_trunc,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_5,'[X]$','') , 3 ) AS S_COD_CODE_5_trunc,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_6,'[X]$','') , 3 ) AS S_COD_CODE_6_trunc,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_7,'[X]$','') , 3 ) AS S_COD_CODE_7_trunc,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_8,'[X]$','') , 3 ) AS S_COD_CODE_8_trunc,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_9,'[X]$','') , 3 ) AS S_COD_CODE_9_trunc,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_10,'[X]$','') , 3 ) AS S_COD_CODE_10_trunc,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_11,'[X]$','') , 3 ) AS S_COD_CODE_11_trunc,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_12,'[X]$','') , 3 ) AS S_COD_CODE_12_trunc,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_13,'[X]$','') , 3 ) AS S_COD_CODE_13_trunc,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_14,'[X]$','') , 3 ) AS S_COD_CODE_14_trunc,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_15,'[X]$','') , 3 ) AS S_COD_CODE_15_trunc,
# MAGIC    LEFT ( REGEXP_REPLACE(S_UNDERLYING_COD_ICD10,'[X]$','') , 4 ) AS S_UNDERLYING_COD_ICD10_trunc_4,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_1,'[X]$','') , 4 ) AS S_COD_CODE_1_trunc_4,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_2,'[X]$','') , 4 ) AS S_COD_CODE_2_trunc_4,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_3,'[X]$','') , 4 ) AS S_COD_CODE_3_trunc_4,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_4,'[X]$','') , 4 ) AS S_COD_CODE_4_trunc_4,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_5,'[X]$','') , 4 ) AS S_COD_CODE_5_trunc_4,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_6,'[X]$','') , 4 ) AS S_COD_CODE_6_trunc_4,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_7,'[X]$','') , 4 ) AS S_COD_CODE_7_trunc_4,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_8,'[X]$','') , 4 ) AS S_COD_CODE_8_trunc_4,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_9,'[X]$','') , 4 ) AS S_COD_CODE_9_trunc_4,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_10,'[X]$','') , 4 ) AS S_COD_CODE_10_trunc_4,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_11,'[X]$','') , 4 ) AS S_COD_CODE_11_trunc_4,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_12,'[X]$','') , 4 ) AS S_COD_CODE_12_trunc_4,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_13,'[X]$','') , 4 ) AS S_COD_CODE_13_trunc_4,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_14,'[X]$','') , 4 ) AS S_COD_CODE_14_trunc_4,
# MAGIC    LEFT ( REGEXP_REPLACE(S_COD_CODE_15,'[X]$','') , 4 ) AS S_COD_CODE_15_trunc_4
# MAGIC   FROM global_temp.ccu035_01_infection_deaths
# MAGIC   --WHERE REG_DATE_OF_DEATH_FORMATTED > '2019-12-31' AND REG_DATE_OF_DEATH_FORMATTED < '2020-12-08'
# MAGIC   )
# MAGIC ),
# MAGIC 
# MAGIC cte_deathcvd_link as (
# MAGIC select *
# MAGIC from cte_deathcvd t1
# MAGIC inner join global_temp.ccu035_01_infection_deaths_outcomes_map t2 on 
# MAGIC (t1.S_UNDERLYING_COD_ICD10_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_1_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_2_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_3_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_4_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_5_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_6_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_7_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_8_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_9_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_10_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_11_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_12_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_13_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_14_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_15_trunc = t2.ICD10code_trunc
# MAGIC OR t1.S_UNDERLYING_COD_ICD10_trunc_4 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_1_trunc_4 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_2_trunc_4 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_3_trunc_4 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_4_trunc_4 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_5_trunc_4 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_6_trunc_4 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_7_trunc_4 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_8_trunc_4 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_9_trunc_4 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_10_trunc_4 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_11_trunc_4 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_12_trunc_4 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_13_trunc_4 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_14_trunc_4 = t2.ICD10code_trunc
# MAGIC OR t1.S_COD_CODE_15_trunc_4 = t2.ICD10code_trunc
# MAGIC )
# MAGIC )
# MAGIC 
# MAGIC select tab1.ID, tab1.REG_DATE_OF_DEATH_FORMATTED, tab2.DEC_CONF_NHS_NUMBER_CLEAN_DEID, tab2.name, tab2.term, tab2.ICD10code_trunc as code 
# MAGIC from cte_anydeath_latest tab1
# MAGIC left join cte_deathcvd_link tab2 on tab1.ID = tab2.DEC_CONF_NHS_NUMBER_CLEAN_DEID 

# COMMAND ----------

tmp2=spark.table(f'global_temp.ccu035_01_infection_death_outcomes')\
  .withColumnRenamed('ID', 'NHS_NUMBER_DEID')\
  .join(index_dates, on='NHS_NUMBER_DEID', how='inner')\
  .where((f.col('covid19_confirmed_date') < f.col('REG_DATE_OF_DEATH_FORMATTED')) & (f.col('end_date') >= f.col('REG_DATE_OF_DEATH_FORMATTED')))
#display(tmp2)
tmp.createOrReplaceGlobalTempView(f'{project_prefix}infection_death_outcomes')

# COMMAND ----------

count_var(tmp2, 'NHS_NUMBER_DEID')

# COMMAND ----------

# MAGIC %sql
# MAGIC ---Diseases as number of records
# MAGIC SELECT count(Distinct NHS_NUMBER_DEID) as count, name
# MAGIC FROM global_temp.ccu035_01_infection_death_outcomes
# MAGIC group by name
# MAGIC order by count desc 

# COMMAND ----------

create_table('ccu035_01_infection_death_outcomes' , select_sql_script=f"SELECT * FROM global_temp.ccu035_01_infection_death_outcomes") 

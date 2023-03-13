# Databricks notebook source
# MAGIC %md #Combined primary care, HES and death CVD outcomes (work package 2.5)
# MAGIC Description This notebooks make a table ccu035_01_cvd_outcomes which contains all observations containing outcomes clinical codes from the HES APC, GDPPR and death dataset, and ccu035_01_cvd_outcomes_first which contains the first date of each phenotype reported in either HES APC, GDPPR or death dataset for for each participant . Phenotypes used are listed below:   
# MAGIC 
# MAGIC 
# MAGIC Project(s) CCU035_01
# MAGIC 
# MAGIC Author(s) Spencer Keene adapted from Rachel Denholm's notebooks. Updated by Hannah Whittaker for project ccu035_01.
# MAGIC 
# MAGIC Reviewer(s)
# MAGIC 
# MAGIC Date last updated 2022-03-24
# MAGIC 
# MAGIC Date last reviewed
# MAGIC 
# MAGIC Date last run
# MAGIC 
# MAGIC Data input GDPPR, HES APC, death
# MAGIC 
# MAGIC Data output table: ccu035_01_cvd_outcomes; ccu035_01_cvd_outcomes_first 
# MAGIC 
# MAGIC Software and versions SQL
# MAGIC 
# MAGIC Packages and versions 'Not applicable'

# COMMAND ----------

# MAGIC %md
# MAGIC This notebook will create a single table (ccu035_01_cvd_outcomes) that contains all recorded outcomes from GDPPR/HES/death
# MAGIC 
# MAGIC |Column | Content|
# MAGIC |----------------|--------------------|
# MAGIC |NHS_NUMBER_DEID | Patient NHS Number |
# MAGIC |record_date | Date of event: date in GDPPR, epistart HES |
# MAGIC |name | Disease group |
# MAGIC |term | Description of disease |
# MAGIC |Arterial_event | 0;1 |
# MAGIC |Venous_event | 0;1 |
# MAGIC |source | gdppr; hes_apc; death |
# MAGIC |terminology | code used | 
# MAGIC |code | clinical code |

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS global_temp.ccu035_01_final_analysis_cohort;
# MAGIC DROP TABLE IF EXISTS ars_nic_391419_j3w9t_collab.ccu035_01_final_analysis_cohort

# COMMAND ----------

# MAGIC %sql
# MAGIC ----- Joining all primary care, hes apc and death outcome data, and creating two summary outcome measures
# MAGIC 
# MAGIC --CREATE TABLE dars_nic_391419_j3w9t_collab.ccu035_01_cvdoutcomes_infections AS
# MAGIC create or replace global temp view ccu035_01_cvdoutcomes_infections as 
# MAGIC 
# MAGIC with cte_alloutcomes as (
# MAGIC SELECT NHS_NUMBER_DEID, DATE as record_date, name, term, "gdppr" AS SOURCE, terminology, code
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu035_01_infection_cvd_outcomes_gdppr
# MAGIC UNION
# MAGIC SELECT NHS_NUMBER_DEID , EPISTART as record_date, name, term, 'hes_apc' as SOURCE, terminology, code
# MAGIC --FROM dars_nic_391419_j3w9t_collab.CCU035_01_infection_hesapcoutcomes
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu035_01_infection_hesapcoutcomes_first_diagnosis
# MAGIC UNION
# MAGIC SELECT NHS_NUMBER_DEID , REG_DATE_OF_DEATH_FORMATTED as record_date, name, term, 'death' as SOURCE, "icd10" as terminology, code
# MAGIC --FROM dars_nic_391419_j3w9t_collab.ccu035_01_infection_death_outcomes
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu035_01_infection_death_outcomes_first_position
# MAGIC where name is not null 
# MAGIC )
# MAGIC 
# MAGIC SELECT NHS_NUMBER_DEID, record_date, name, term, SOURCE, terminology, code
# MAGIC --case when name = 'AMI' OR name = 'stroke_isch' OR name = 'stroke_NOS' OR name = 'other_arterial_embolism' THEN 1 Else 0 End as Arterial_event,
# MAGIC --case when name = 'PE' OR name = '%DVT%' OR name = '%ICVT%' OR name = 'PE%' THEN 1 Else 0 End as Venous_event,
# MAGIC --case when name like 'DIC' OR name = 'TTP' OR name = 'thrombocytopenia' THEN 1 Else 0 End as Haematological_event
# MAGIC from cte_alloutcomes

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(Distinct NHS_NUMBER_DEID) as count
# MAGIC FROM global_temp.ccu035_01_cvdoutcomes_infections

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.ccu035_01_cvdoutcomes_infections

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- First event of each phenotype
# MAGIC 
# MAGIC CREATE or replace global temp view  ccu035_01_cvdoutcomes_infections_first AS
# MAGIC --create or replace global temp view ccu035_01_cvdoutcomes_infections_first as
# MAGIC 
# MAGIC with cte_names as (
# MAGIC     SELECT NHS_NUMBER_DEID, record_date, name, term, SOURCE, terminology, code, 
# MAGIC     ROW_NUMBER() OVER (PARTITION BY NHS_NUMBER_DEID, name ORDER BY record_date) AS seq
# MAGIC --FROM dars_nic_391419_j3w9t_collab.ccu035_01_cvdoutcomes_infections 
# MAGIC FROM global_temp.ccu035_01_cvdoutcomes_infections
# MAGIC )
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM cte_names WHERE seq = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC ----Number of patient with first event of each disease
# MAGIC 
# MAGIC SELECT count(Distinct NHS_NUMBER_DEID) as count, name
# MAGIC --FROM dars_nic_391419_j3w9t_collab.ccu035_01_cvdoutcomes_first
# MAGIC from global_temp.ccu035_01_cvdoutcomes_infections_first
# MAGIC group by name
# MAGIC order by count desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.ccu035_01_cvdoutcomes_infections_first

# COMMAND ----------

# MAGIC %sql
# MAGIC --- First event of summary outcomes (venous/arterial event)
# MAGIC 
# MAGIC --CREATE TABLE dars_nic_391419_j3w9t_collab.ccu035_01_cvdoutcome_infections_summary_first AS
# MAGIC --create or replace global temp view ccu035_01_cvdoutcome_infections_summary_first as
# MAGIC 
# MAGIC /*with cte_arterial as (
# MAGIC SELECT NHS_NUMBER_DEID, record_date as arterial_date, Arterial_event, SOURCE as art_source, ROW_NUMBER()
# MAGIC OVER (PARTITION BY NHS_NUMBER_DEID, Arterial_event ORDER BY record_date) AS seq
# MAGIC FROM global_temp.ccu035_01_cvdoutcomes_infections 
# MAGIC )
# MAGIC ,
# MAGIC 
# MAGIC cte_firstarterial as 
# MAGIC (SELECT *
# MAGIC FROM cte_arterial WHERE seq = 1
# MAGIC ),
# MAGIC 
# MAGIC cte_venous as (
# MAGIC SELECT NHS_NUMBER_DEID as ID, record_date as venous_date, Venous_event, SOURCE as ven_source, ROW_NUMBER()
# MAGIC OVER(PARTITION BY NHS_NUMBER_DEID, Venous_event ORDER BY record_date) AS seq
# MAGIC FROM global_temp.ccu035_01_cvdoutcomes_infections 
# MAGIC ),
# MAGIC 
# MAGIC cte_firstvenous as 
# MAGIC (SELECT *
# MAGIC FROM cte_venous WHERE seq = 1
# MAGIC ),
# MAGIC 
# MAGIC cte_haemato as (
# MAGIC SELECT NHS_NUMBER_DEID as ID3, record_date as haematological_date, Haematological_event, SOURCE as hemato_source, ROW_NUMBER()
# MAGIC OVER(PARTITION BY NHS_NUMBER_DEID, Haematological_event ORDER BY record_date) AS seq
# MAGIC FROM global_temp.ccu035_01_cvdoutcomes_infections 
# MAGIC ),
# MAGIC 
# MAGIC cte_firsthaemato as 
# MAGIC (SELECT *
# MAGIC FROM cte_haemato WHERE seq = 1
# MAGIC )
# MAGIC 
# MAGIC SELECT tab1.NHS_NUMBER_DEID, tab1.arterial_date, tab1.Arterial_event, tab1.art_source, tab2.ID, tab2.venous_date, tab2.Venous_event, tab2.ven_source, tab3.ID3, tab3.haematological_date, tab3.Haematological_event, tab3.hemato_source
# MAGIC FROM cte_firstarterial tab1
# MAGIC FULL OUTER JOIN cte_firstvenous tab2
# MAGIC ON tab1.NHS_NUMBER_DEID = tab2.ID
# MAGIC FULL OUTER JOIN cte_firsthaemato tab3
# MAGIC ON tab1.NHS_NUMBER_DEID = tab3.ID3

# COMMAND ----------

# MAGIC %sql
# MAGIC ----Number of patient with first event of each disease by source of data
# MAGIC 
# MAGIC /*SELECT count(Distinct NHS_NUMBER_DEID) as count, name, SOURCE
# MAGIC --FROM dars_nic_391419_j3w9t_collab.ccu035_01_cvdoutcomes_first
# MAGIC from global_temp.ccu035_01_cvdoutcomes_infections_first
# MAGIC group by name, SOURCE
# MAGIC order by name desc 

# COMMAND ----------

# MAGIC %sql
# MAGIC /*select * from global_temp.ccu035_01_cvdoutcome_infections_summary_first

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Summary outcome measures
# MAGIC /*SELECT SUM(CASE WHEN Arterial_event = 1 then 1 else 0 end) as arterial,
# MAGIC       SUM(CASE WHEN Venous_event = 1 then 1 else 0 end) as venous 
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu035_01_cvdoutcome_infections_summary_first

# COMMAND ----------

# MAGIC %sql
# MAGIC /*SELECT * FROM dars_nic_391419_j3w9t_collab.ccu035_01_cvdoutcome_infections_summary_first

# COMMAND ----------

# MAGIC %run "/Workspaces/dars_nic_391419_j3w9t_collab/SHDS/common/functions"

# COMMAND ----------

outcomes = spark.table(f'global_temp.ccu035_01_cvdoutcomes_infections_first')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.ccu035_01_cvdoutcomes_infections_first

# COMMAND ----------

#merge covariates table with outcome events table
covariates = spark.table(f'dars_nic_391419_j3w9t_collab.ccu035_01_included_patients_allcovariates')\
  .join(outcomes, on='NHS_NUMBER_DEID', how='left')
covariates.createOrReplaceGlobalTempView(f'ccu035_01_final_analysis_cohort')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.ccu035_01_final_analysis_cohort

# COMMAND ----------

end_date= spark.table(f'dars_nic_391419_j3w9t_collab.ccu035_01_start_and_end_date')


# COMMAND ----------

test=end_date\
  .select('NHS_NUMBER_DEID', 'end_date')

# COMMAND ----------

cohort=covariates\
  .join(test, on='NHS_NUMBER_DEID', how='inner')
cohort.createOrReplaceGlobalTempView(f'ccu035_01_final_analysis_cohort')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.ccu035_01_final_analysis_cohort

# COMMAND ----------

df=spark.table(f'dars_nic_391419_j3w9t_collab.ccu035_01_gdppr_dars_nic_391419_j3w9t')

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from dars_nic_391419_j3w9t_collab.ccu035_01_gdppr_dars_nic_391419_j3w9t

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- First event of each phenotype
# MAGIC 
# MAGIC CREATE or replace global temp view  ccu035_01_practice AS
# MAGIC 
# MAGIC with cte_names as (
# MAGIC     SELECT NHS_NUMBER_DEID, PRACTICE, DATE,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY NHS_NUMBER_DEID, NHS_NUMBER_DEID ORDER BY DATE DESC) AS seq
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu035_01_gdppr_dars_nic_391419_j3w9t
# MAGIC )
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM cte_names WHERE seq = 1

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from global_temp.ccu035_01_practice

# COMMAND ----------

df=spark.table(f'global_temp.ccu035_01_practice')\
  .select ('NHS_NUMBER_DEID', 'PRACTICE')

df2=spark.table(f'global_temp.ccu035_01_final_analysis_cohort')\
  .join(df, on='NHS_NUMBER_DEID', how='inner')
df2.createOrReplaceGlobalTempView(f'ccu035_01_final_analysis_cohort')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.ccu035_01_final_analysis_cohort

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dars_nic_391419_j3w9t_collab.ccu035_01_final_analysis_cohort

# COMMAND ----------

# MAGIC %sql
# MAGIC create table dars_nic_391419_j3w9t_collab.ccu035_01_final_analysis_cohort as
# MAGIC select *
# MAGIC from global_temp.ccu035_01_final_analysis_cohort

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(Distinct NHS_NUMBER_DEID) as count
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu035_01_final_analysis_cohort

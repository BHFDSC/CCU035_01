-- Databricks notebook source
-- MAGIC %md # CCU035_01-D10-vaccine_status
-- MAGIC  
-- MAGIC **Description** This notebook generates a vaccine status table.
-- MAGIC 
-- MAGIC **Author(s)** Spencer Keene, Jenny Cooper (Edited by Hannah Whittaker for project ccu035)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC 
-- MAGIC #Dataset parameters
-- MAGIC vaccine_data = 'dars_nic_391419_j3w9t_collab.ccu035_01_vaccine_status_dars_nic_391419_j3w9t'
-- MAGIC 
-- MAGIC #Other data inputs
-- MAGIC skinny_QA_inclusion_table = 'dars_nic_391419_j3w9t_collab.ccu035_01_inf_included_patients' 
-- MAGIC #skinny patient table that has undergone QA and inclusion/exclusion
-- MAGIC 
-- MAGIC #Final table
-- MAGIC 
-- MAGIC final_table = 'ccu035_01_vac_vaccine_status'

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu035_01_codelist_covid_vaccine_products AS
-- MAGIC SELECT *
-- MAGIC FROM VALUES
-- MAGIC ("covid vaccination", "SNOMED","39114911000001105","COVID19_vaccine_AstraZeneca", "1","n/a"),
-- MAGIC ("covid vaccination", "SNOMED","39115011000001105","COVID19_vaccine_AstraZeneca", "1","n/a"),
-- MAGIC ("covid vaccination", "SNOMED","39115111000001106","COVID19_vaccine_AstraZeneca", "1","n/a"),
-- MAGIC ("covid vaccination", "SNOMED","39115711000001107","COVID19_vaccine_Pfizer", "1","n/a"),
-- MAGIC ("covid vaccination", "SNOMED","39115611000001103","COVID19_vaccine_Pfizer", "1","n/a"),
-- MAGIC ("covid vaccination", "SNOMED","39326911000001101","COVID19_vaccine_Moderna", "1","n/a"), 
-- MAGIC ("covid vaccination", "SNOMED","39375411000001104","COVID19_vaccine_Moderna", "1","n/a"),
-- MAGIC ("covid vaccination", "SNOMED","1324681000000101","COVID19_vaccine_dose1", "1","n/a"),
-- MAGIC ("covid vaccination", "SNOMED","1324691000000104","COVID19_vaccine_dose2", "1","n/a"),
-- MAGIC ("covid vaccination", "SNOMED","61396006","COVID19_vaccine_site_left_thigh", "1","n/a"),
-- MAGIC ("covid vaccination", "SNOMED","368209003","COVID19_vaccine_site_right_upper_arm", "1","n/a"),
-- MAGIC ("covid vaccination", "SNOMED","368208006","COVID19_vaccine_site_left_upper_arm", "1","n/a"),
-- MAGIC ("covid vaccination", "SNOMED","723980000","COVID19_vaccine_site_right_buttock", "1","n/a"),
-- MAGIC ("covid vaccination", "SNOMED","723979003","COVID19_vaccine_site_left_buttock", "1","n/a"),
-- MAGIC ("covid vaccination", "SNOMED","11207009","COVID19_vaccine_site_right_thigh", "1","n/a"),
-- MAGIC ("covid vaccination", "SNOMED","413294000","COVID19_vaccine_care_setting_community_health_services", "1","n/a"),
-- MAGIC ("covid vaccination", "SNOMED","310065000","COVID19_vaccine_care_setting_open_access_service", "1","n/a"),
-- MAGIC ("covid vaccination", "SNOMED","788007007","COVID19_vaccine_care_setting_general_practice_service", "1","n/a"),
-- MAGIC ("covid vaccination", "SNOMED", "1324671000000103", "COVID19_vaccine_dose3", "1", "n/a"),
-- MAGIC ("covid vaccination", "SNOMED", "1362591000000103", "COVID19_vaccine_dose_booster","1", "n/a")

-- COMMAND ----------

#select VACCINE_PRODUCT_CODE from dars_nic_391419_j3w9t_collab.ccu035_01_vaccine_status_dars_nic_391419_j3w9t
#group by VACCINE_PRODUCT_CODE

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC from pyspark.sql.functions import countDistinct, year, dayofmonth, from_unixtime, month, unix_timestamp, to_timestamp, date_format, col, datediff, to_date, lit, months_between
-- MAGIC import pandas as pd
-- MAGIC import numpy as np
-- MAGIC import seaborn as sns
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC import datetime
-- MAGIC from pyspark.sql.functions import *
-- MAGIC import pyspark.sql.functions as f
-- MAGIC from pyspark.sql import Window
-- MAGIC import io
-- MAGIC from functools import reduce
-- MAGIC from pyspark.sql.types import StringType

-- COMMAND ----------

-- MAGIC %py
-- MAGIC 
-- MAGIC #PROBABLY THE MOST IMPORTANT COLUMNS FROM VACCINE STATUS TABLE
-- MAGIC spark.sql(F"""create or replace global temp view vaccine_status_table as
-- MAGIC select * from {vaccine_data}""") 

-- COMMAND ----------

-- MAGIC %md ## Codes to values

-- COMMAND ----------

SELECT count(DISTINCT PERSON_ID_DEID) as count, VACCINATION_PROCEDURE_CODE --VACCINE_PRODUCT_CODE
--FROM global_temp.vaccine5
from global_temp.vaccine_status_table
group by VACCINATION_PROCEDURE_CODE --VACCINE_PRODUCT_CODE
order by count desc

-- COMMAND ----------

create or replace global temp view vaccine1 as
SELECT *,
    CASE 
        WHEN VACCINE_PRODUCT_CODE IN ('39114911000001105', '39115011000001105', '39115111000001106') THEN 'AstraZeneca'
        WHEN VACCINE_PRODUCT_CODE IN ('39115711000001107', '39115611000001103') THEN 'Pfizer'
        WHEN VACCINE_PRODUCT_CODE IN ('39326911000001101', '39375411000001104') THEN 'Moderna' END as vaccine_product
    
from global_temp.vaccine_status_table 
    

-- COMMAND ----------

create or replace global temp view vaccine2 as
SELECT *,
    CASE 
        WHEN VACCINATION_PROCEDURE_CODE IN ('1324691000000104') THEN 'second dose' 
        WHEN VACCINATION_PROCEDURE_CODE IN ('1324681000000101') THEN 'first dose' 
        WHEN VACCINATION_PROCEDURE_CODE IN ('1324671000000103') THEN 'third dose' 
        WHEN VACCINATION_PROCEDURE_CODE IN ('1362591000000103') THEN 'booster dose' END AS vaccine_dose
from global_temp.vaccine1

-- COMMAND ----------

create or replace global temp view vaccine3 as
SELECT *,
    CASE        
        WHEN VACCINATION_SITUATION_CODE IN ('1324741000000101') THEN 'first dose declined' 
        WHEN VACCINATION_SITUATION_CODE IN ('1324751000000103') THEN 'second dose declined' END AS declined
from global_temp.vaccine2

-- COMMAND ----------

create or replace global temp view vaccine4 as
SELECT *,
    CASE 
        WHEN SITE_OF_VACCINATION_CODE IN ('61396006') THEN 'left thigh'
        WHEN SITE_OF_VACCINATION_CODE IN ('368209003') THEN 'right upper arm'
        WHEN SITE_OF_VACCINATION_CODE IN ('368208006') THEN 'left upper arm'
        WHEN SITE_OF_VACCINATION_CODE IN ('723980000') THEN 'right buttock'
        WHEN SITE_OF_VACCINATION_CODE IN ('723979003') THEN 'left buttock'
        WHEN SITE_OF_VACCINATION_CODE IN ('11207009') THEN 'right thigh' END AS site_of_vaccination
from global_temp.vaccine3

-- COMMAND ----------

create or replace global temp view vaccine5 as
SELECT *,
    CASE         
        WHEN CARE_SETTING_TYPE_CODE IN('413294000') THEN 'Community health services'
        WHEN CARE_SETTING_TYPE_CODE IN('310065000') THEN 'Open access service'
        WHEN CARE_SETTING_TYPE_CODE IN('788007007') THEN 'General practice service' END AS care_setting_vaccination
from global_temp.vaccine4

-- COMMAND ----------

create or replace global temp view ccu035_01_vaccine_status as
select PERSON_ID_DEID as NHS_NUMBER_DEID, /*MYDOB, AGE, POSTCODE_DISTRICT, LSOA, TRACE_VERIFIED,*/ to_date(cast(RECORDED_DATE as string), 'yyyyMMdd') as VACCINATION_DATE, vaccine_product as VACCINE_PRODUCT, vaccine_dose as VACCINE_DOSE, DOSE_AMOUNT, DOSE_SEQUENCE, NOT_GIVEN, declined as DECLINED, care_setting_vaccination as CARE_SETTING from global_temp.vaccine5
---antijoin

-- COMMAND ----------

select * from global_temp.ccu035_01_vaccine_status

-- COMMAND ----------

-- MAGIC %md ## Getting rid of dupicates and multiple first doses

-- COMMAND ----------



create or replace global temp view ccu035_01_vaccine_status_final AS 

with cte as (
    SELECT *, 
    ROW_NUMBER() OVER (PARTITION BY NHS_NUMBER_DEID, VACCINE_DOSE ORDER BY VACCINATION_DATE ASC) AS seq
                                                                                             
                                                                   
                                                                                           
FROM global_temp.ccu035_01_vaccine_status
)

SELECT *
FROM cte

where seq=1 and VACCINATION_DATE>='2020-12-08' and VACCINATION_DATE<=current_date and DECLINED is NULL 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Define create table function by Sam H
-- MAGIC # Source: Workspaces/dars_nic_391419_j3w9t_collab/DATA_CURATION_wrang000_functions
-- MAGIC 
-- MAGIC def create_table(table_name:str, database_name:str='dars_nic_391419_j3w9t_collab', select_sql_script:str=None) -> None:
-- MAGIC   """Will save to table from a global_temp view of the same name as the supplied table name (if no SQL script is supplied)
-- MAGIC   Otherwise, can supply a SQL script and this will be used to make the table with the specificed name, in the specifcied database."""
-- MAGIC   
-- MAGIC   spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
-- MAGIC   
-- MAGIC   if select_sql_script is None:
-- MAGIC     select_sql_script = f"SELECT * FROM global_temp.{table_name}"
-- MAGIC   
-- MAGIC   spark.sql(f"""CREATE TABLE {database_name}.{table_name} AS
-- MAGIC                 {select_sql_script}
-- MAGIC              """)
-- MAGIC   spark.sql(f"ALTER TABLE {database_name}.{table_name} OWNER TO {database_name}")
-- MAGIC   
-- MAGIC def drop_table(table_name:str, database_name:str='dars_nic_391419_j3w9t_collab', if_exists=True):
-- MAGIC   if if_exists:
-- MAGIC     IF_EXISTS = 'IF EXISTS'
-- MAGIC   else: 
-- MAGIC     IF_EXISTS = ''
-- MAGIC   spark.sql(f"DROP TABLE {IF_EXISTS} {database_name}.{table_name}")

-- COMMAND ----------

create or replace global temp view ccu035_01_vaccine_status_final_temp as
select NHS_NUMBER_DEID, VACCINATION_DATE, VACCINE_PRODUCT, VACCINE_DOSE
from global_temp.ccu035_01_vaccine_status_final

-- COMMAND ----------

select * from global_temp.ccu035_01_vaccine_status_final_temp

-- COMMAND ----------

drop table if exists dars_nic_391419_j3w9t_collab.ccu035_01_vac_vaccine_status

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC #create_table(table_name=final_table, database_name='dars_nic_391419_j3w9t_collab', select_sql_script=f"SELECT * FROM global_temp.ccu035_01_vaccine_status_final_temp")
-- MAGIC create_table('ccu035_01'+ 'vaccine_all' , select_sql_script=f"SELECT * FROM global_temp.ccu035_01_vaccine_status_final_temp")
-- MAGIC #final_table is a parameter name defined at the start so can be used for infection and vaccine

-- COMMAND ----------

select * from dars_nic_391419_j3w9t_collab.ccu035_01_vac_vaccine_status

-- COMMAND ----------

-- MAGIC %md ## Vaccine Interval and mixed doses

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW ccu035_01_vaccine_interval as
/*with cte_interval as (SELECT NHS_NUMBER_DEID, VACCINE_DOSE, VACCINATION_DATE, VACCINE_PRODUCT FROM dars_nic_391419_j3w9t_collab.ccu035_01_vacc_vaccine_status_final
)
*/
SELECT tbl1.NHS_NUMBER_DEID, tbl1.VACCINE_PRODUCT as vaccine_prod1, tbl1.VACCINE_DOSE as vaccine_1_dose, tbl1.VACCINATION_DATE as vaccine_1_date, 
                             tbl2.VACCINE_PRODUCT as vaccine_prod2, tbl2.VACCINE_DOSE as vaccine_2_dose, tbl2.VACCINATION_DATE as vaccine_2_date
from 
   --global_temp.ccu035_01_vacc_vaccine_status_final_temp as tbl1 
     dars_nic_391419_j3w9t_collab.ccu035_01_vac_vaccine_status as tbl1
LEFT JOIN 
     --global_temp.ccu035_01_vacc_vaccine_status_final_temp as tbl2
     dars_nic_391419_j3w9t_collab.ccu035_01_vac_vaccine_status as tbl2
     on tbl1.NHS_NUMBER_DEID = tbl2.NHS_NUMBER_DEID
WHERE tbl1.VACCINE_DOSE != tbl2.VACCINE_DOSE and tbl1.VACCINE_DOSE='first dose' --and tbl1.VACCINE_DOSE='second_dose'

-- COMMAND ----------

select * from global_temp.ccu035_01_vaccine_interval


-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW ccu035_01_vaccine_interval_final as
SELECT * --, Vaccine_Date_diff
FROM
  (select *, (DATEDIFF(vaccine_2_date, vaccine_1_date)) as Vaccine_Date_diff 
  from global_temp.ccu035_01_vaccine_interval)
where Vaccine_Date_diff>=21
--GROUP BY Vaccine_Date_diff
--ORDER BY Vaccine_Date_diff

-- COMMAND ----------

select * from global_temp.ccu035_01_vaccine_interval_final

-- COMMAND ----------

/*HW-created the table above to use to get an end of follow-up date in the next workspace */
create table dars_nic_391419_j3w9t_collab.ccu035_01_vaccine_status_number as
select NHS_NUMBER_DEID, vaccine_prod1, vaccine_1_dose, vaccine_1_date, vaccine_prod2, vaccine_2_dose, vaccine_2_date, Vaccine_Date_diff
from global_temp.ccu035_01_vaccine_interval_final


-- COMMAND ----------

/*SELECT Vaccine_Date_diff, count(*)
FROM
  (select /*NHS_NUMBER_DEID,*/ (DATEDIFF(vaccine_date_2, vaccine_date_1)) as vaccine_Date_diff 
  from global_temp.ccu035_01_vaccine_interval)
GROUP BY vaccine_Date_diff
ORDER BY VACCine_DATE_DIFF

-- COMMAND ----------

/*WITH CTE AS
(
select /*NHS_NUMBER_DEID,*/ (DATEDIFF(vaccine_date_2, vaccine_date_1)) as vaccine_date_diff 
from global_temp.vaccine_interval
where vaccine_date_1 < '2021-01-01'
)
SELECT Vaccine_Date_diff, count(*)
FROM
  CTE
GROUP BY vaccine_Date_diff
ORDER BY VACCine_DATE_DIFF

-- COMMAND ----------

WITH CTE AS
(
select /*NHS_NUMBER_DEID,*/ (DATEDIFF(vaccine_date_2, vaccine_date_1)) as vaccine_date_diff 
from global_temp.vaccine_interval
where vaccine_date_1 >= '2021-01-01'
)
SELECT Vaccine_Date_diff, count(*)
FROM
  CTE
GROUP BY vaccine_Date_diff
ORDER BY VACCine_DATE_DIFF

-- COMMAND ----------

/*Create or replace global temp view ccu035_01_vaccine_product as

with cte_product as (SELECT *,
(case when vaccine_prod2 like 'Pfizer%' then 'Pfizer'
 when vaccine_prod2 like 'Astra%' then 'Astra'
 when vaccine_prod2 like 'Moderna%' then 'Moderna' end) as Vaccine_Product2
FROM global_temp.ccu035_01_vacc_vaccine_interval_final
),

cte_product2 as (select *, 
(case when vaccine_prod1 like 'Pfizer%' then 'Pfizer'
 when vaccine_prod1 like 'Astra%' then 'Astra'
 when vaccine_prod1 like 'Moderna%' then 'Moderna' end) as Vaccine_Product1
from cte_product)

--Create or replace global temp view ccu035_01_vaccine_product as
Select *
, CASE WHEN Vaccine_Product1 = Vaccine_Product2 THEN 'Same' ELSE 'Mixed' END AS Dosing
from cte_product2



-- COMMAND ----------

/*select * from global_temp.ccu035_01_vaccine_product

-- COMMAND ----------

/*Create or replace global temp view ccu035_01_vaccine_product2 as
select * 
from global_temp.ccu035_01_vaccine_product
where Dosing='Same' OR (Dosing='Mixed' and vaccine_2_date>='2021-05-07')

-- COMMAND ----------

/*create table dars_nic_391419_j3w9t_collab.ccu035_01_vac_vaccine_status_additional_analysis as

select NHS_NUMBER_DEID, vaccine_prod1, vaccine_1_dose, vaccine_1_date, vaccine_prod2, vaccine_2_dose, vaccine_2_date, Vaccine_Date_diff, Dosing  
--count(NHS_NUMBER_DEID) as count, DOSING 
from global_temp.ccu035_01_vaccine_product2
--group by DOSING
--where Dosing='Mixed'

-- COMMAND ----------

/*create or replace global temp view ccu035_01_vaccine_loyal as

with cte_loyal as (select *, 
(case when Vaccine_Product1= 'Pfizer' and Vaccine_Product2= 'Pfizer' then 'both Pfizer'
when Vaccine_Product1= 'Astra' and Vaccine_Product2= 'Astra' then 'both AstraZenecca'
when Vaccine_Product1= 'Astra' and Vaccine_Product2= 'Pfizer' then 'Astra then Pfizer'
when Vaccine_Product1= 'Pfizer' and Vaccine_Product2= 'Astra' then 'Pfizer then Astra' END) AS Vaccine_loyalty
from global_temp.ccu035_01_vaccine_product)

select NHS_NUMBER_DEID, Vaccine_loyalty from cte_loyal



-- COMMAND ----------

/*select * from global_temp.ccu035_01_vaccine_loyal

-- COMMAND ----------

/*WITH CTE AS
(
select /*NHS_NUMBER_DEID,*/ (DATEDIFF(vaccine_date_2, vaccine_date_1)) as vaccine_date_diff 
from global_temp.ccu035_01_vaccine_product
where Vaccine_Product2 like 'Pfizer%'
)
SELECT Vaccine_Date_diff, count(*)
FROM
  CTE
GROUP BY vaccine_Date_diff
ORDER BY VACCine_DATE_DIFF

-- COMMAND ----------

/*WITH CTE AS
(
select /*NHS_NUMBER_DEID,*/ (DATEDIFF(vaccine_date_2, vaccine_date_1)) as vaccine_date_diff 
from global_temp.ccu035_01_vaccine_product
where Vaccine_Product2 like 'Astra%'
)
SELECT Vaccine_Date_diff, count(*)
FROM
  CTE
GROUP BY vaccine_Date_diff
ORDER BY VACCine_DATE_DIFF

-- COMMAND ----------

/*select /*NHS_NUMBER_DEID,*/ (DATEDIFF(vaccine_date_2, vaccine_date_1)) as vaccine_date_diff 
from global_temp.ccu035_01_vaccine_product
where Vaccine_Product2 like 'Astra%'

-- COMMAND ----------

/*select count(distinct NHS_NUMBER_DEID) as count, Vaccine_loyalty
from global_temp.ccu035_01_vaccine_loyal 
group by Vaccine_loyalty
order by count desc

-- COMMAND ----------

--drop table if exists dars_nic_391419_j3w9t_collab.ccu035_01_vacc_vaccine_status_final

-- COMMAND ----------

-- MAGIC %md ## Other important figures

-- COMMAND ----------

/*SELECT count(DISTINCT PERSON_ID_DEID) as count, RECORDED_DATE
FROM global_temp.vaccine5
group by RECORDED_DATE
order by RECORDED_DATE asc

-- COMMAND ----------

/*SELECT count(DISTINCT PERSON_ID_DEID) as count, age
FROM global_temp.vaccine5
group by age
order by age asc



-- COMMAND ----------

/*SELECT count(DISTINCT NHS_NUMBER_DEID) as count, vaccine_product
--FROM global_temp.vaccine5
from global_temp.ccu035_01_vacc_vaccine_status_final
group by vaccine_product
order by count desc

-- COMMAND ----------

/*SELECT count(DISTINCT PERSON_ID_DEID) as count, declined
FROM global_temp.vaccine5
group by declined
order by count desc

-- COMMAND ----------

/*SELECT count(Distinct PERSON_ID_DEID) as count, care_setting_vaccination
FROM global_temp.vaccine5
group by care_setting_vaccination
order by count desc

-- COMMAND ----------

/*SELECT count(Distinct PERSON_ID_DEID) as count, site_of_vaccination
FROM global_temp.vaccine5
group by site_of_vaccination
order by count desc

-- COMMAND ----------

/*SELECT count(PERSON_ID_DEID) as count, vaccine_dose
FROM global_temp.vaccine5
group by vaccine_dose
order by count desc

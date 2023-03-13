# Databricks notebook source
# MAGIC %md # CCU035_01-D05-quality_assurance
# MAGIC  
# MAGIC **Description** This notebook creates a register and applies a series of quality assurance steps to a cohort of data (Skinny Cohort table) of NHS Numbers to potentially remove from analyses due to conflicting data, with reference to previous work/coding by Spiros.
# MAGIC 
# MAGIC **Author(s)** Updated by Rochelle Knight for CCU002_01 from work by Jennifer Cooper and Samantha Ip for CCU002_02. Run by Hannah Whittaker for CCU035_01.
# MAGIC  
# MAGIC **Reviewer(s)** UNREVIEWED
# MAGIC  
# MAGIC **Date last updated** 16/02/2022
# MAGIC  
# MAGIC **Date last reviewed** 
# MAGIC  
# MAGIC **Date last run** 16/02/2022
# MAGIC  
# MAGIC **Data input** `ddars_nic_391419_j3w9t_collab.ccu035_01_inf_skinny_patient` 
# MAGIC 
# MAGIC `dars_nic_391419_j3w9t_collab.ccu035_01_deaths_dars_nic_391419_j3w9t`
# MAGIC 
# MAGIC `dars_nic_391419_j3w9t_collab.ccu035_01_gdppr_dars_nic_391419_j3w9t`
# MAGIC 
# MAGIC **Coding Lists** Pregnancy related codes  (these were identified by searching all available codes in the GDPPR dictionary, R code for searching dictionary is included on JC GitHub) , Prostate cancer codes   (Caliber, I reviewed this list after read to snomed conversion so only contains applicable codes)
# MAGIC 
# MAGIC **Data output** `dars_nic_391419_j3w9t_collab.ccu035_01_inf_conflicting_patients`
# MAGIC 
# MAGIC **Software and versions** Python, SQL
# MAGIC  
# MAGIC **Packages and versions** pandas, pprint, numpy, from pyspark.sql.types import StringType 
# MAGIC 
# MAGIC 
# MAGIC **Extra Comments** 
# MAGIC 1. More QA/amendments will need to be taken into account if more variables are added to the skinny table
# MAGIC 2. Could add extra category onto skinny table with 1/0 for the following rules and then people can remove if they want to
# MAGIC 3. Difference between removing events for people versus removing a person. Events can be removed for individual Analyses.

# COMMAND ----------

# MAGIC %run "/Workspaces/dars_nic_391419_j3w9t_collab/CCU035_01/CCU035_01-functions/wrang000_functions"

# COMMAND ----------

# MAGIC %run "/Workspaces/dars_nic_391419_j3w9t_collab/CCU035_01/CCU035_01-D15-master_notebook_parameters"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Infection parameters

# COMMAND ----------

#Dataset Parameters (needed for the GDPPR presence lookup)
gdppr_data = 'gdppr_dars_nic_391419_j3w9t'
skinny_data = 'inf_skinny_patient'
deaths_data = 'deaths_dars_nic_391419_j3w9t'


#Final table name
collab_database_name = 'dars_nic_391419_j3w9t_collab'
project_prefix = 'ccu035_01_'
QA_table_name = 'inf_skinny_conflicting_patients' 

# COMMAND ----------

#get the latest version of the skinny table
spark.sql(F"""REFRESH TABLE {collab_database_name}.{project_prefix}{skinny_data}""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## functions

# COMMAND ----------

#Python mods
import pandas as pd
from pprint import pprint
import datetime
from pyspark.sql.functions import countDistinct, year, dayofmonth, from_unixtime, month, unix_timestamp, to_timestamp, date_format, col, datediff, to_date, lit, months_between, when, current_date


# COMMAND ----------

#all_conflicting_patients = [ ]

#def flag_patients(eids):
#  """
#  Add patients to patient register.
#  """
#  all_conflicting_patients.extend(eids)
#  print(f"Added {len(set(eids))} unique patients to register - {len(set(all_conflicting_patients))} unique patients total" )

#def examine_rows_column_value(df, colname, value):
#  if value is None:
#      tmp_df = df.where(col(colname).isNull())
#  else:
#      tmp_df = df[df[colname] == value]
#  display(tmp_df)
  
#def find_dupes(df, id_colname):
#  display(df.groupBy(id_colname).count().where("count > 1").drop("count"))
  
#def count_unique_pats(df, id_colname):
#  n_unique_pats = df.agg(countDistinct(id_colname)).toPandas()
#  return int(n_unique_pats.values)

#def create_table(df, table_name:str, database_name:str="dars_nic_391419_j3w9t_collab", select_sql_script:str=None) -> None:
#  """Will save to table from a global_temp view of the same name as the supplied table name (if no SQL script is supplied)
#  Otherwise, can supply a SQL script and this will be used to make the table with the specificed name, in the specifieda database."""
#  spark.sql(f"""DROP TABLE IF EXISTS {database_name}.{table_name}""")
#  df.createOrReplaceGlobalTempView(table_name)
#  spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
#  if select_sql_script is None:
#    select_sql_script = f"SELECT * FROM global_temp.{table_name}"
#  spark.sql(f"""CREATE TABLE {database_name}.{table_name} AS
#                {select_sql_script}""")
#  spark.sql(f"""
#                ALTER TABLE {database_name}.{table_name} OWNER TO {database_name}
#             """)

# COMMAND ----------

# MAGIC %run "/Workspaces/dars_nic_391419_j3w9t_collab/CCU035_01/CCU035_01-D01-codelist"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule 1: Year of birth is after the year of death

# COMMAND ----------

df_skinny = spark.sql(f"""SELECT *, YEAR(date_of_birth) as yob, YEAR(date_of_death) as yod
FROM {collab_database_name}.{project_prefix}{skinny_data}""")

rule_1_sql = 'CASE WHEN yob > yod THEN 1 ELSE 0 END as rule_1'
  

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule 2: Patient does not have mandatory fields completed (nhs_number, sex, Date of birth)

# COMMAND ----------

rule_2_sql = '''CASE WHEN 
SEX is null OR SEX = " " OR SEX like "" 
OR DATE_OF_BIRTH is null
OR nhs_number_deid is null OR nhs_number_deid = " " OR nhs_number_deid = "" THEN 1 ELSE 0 END as rule_2'''

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Rule 3: Year of Birth Predates NHS Established Year or Year is over the Current Date

# COMMAND ----------

rule_3_sql = f"""CASE WHEN 
yob<1793 ---predates nhs
or yob> '{datetime.datetime.today()}'--YEAR(CURRENT_TIMESTAMP) 
THEN 1 ELSE 0 END as rule_3"""

# COMMAND ----------

df_skinny = df_skinny.selectExpr("*", rule_1_sql, 
                             rule_2_sql,
                            rule_3_sql
                            )

df_skinny.cache().count()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule 4 - Remove those with only null/invalid dates of death

# COMMAND ----------

df_death = spark.sql(f"""SELECT *, to_date(REG_DATE_OF_DEATH, 'yyyyMMdd') as dod FROM {collab_database_name}.{project_prefix}{deaths_data}""")
df_death = df_death.withColumn("rule_4", when(
  (
    (col("dod").isNull()) | (col("dod") <= to_date(lit("1900-01-01"))) | (col("dod") > current_date() )
  ), 1).otherwise(0))
df_death = df_death.select(['DEC_CONF_NHS_NUMBER_CLEAN_DEID', "rule_4"]).groupBy("DEC_CONF_NHS_NUMBER_CLEAN_DEID").agg({"rule_4": "min"}).withColumnRenamed('min(rule_4)', 'rule_4')

# COMMAND ----------

df_rule4 = df_death.select(['DEC_CONF_NHS_NUMBER_CLEAN_DEID', "rule_4"]).filter(col("rule_4")==1)
display(df_rule4)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Rule 5 - Remove those where registered date of death before the actual date of death

# COMMAND ----------

df_death = spark.sql(f"""SELECT *, to_date(REG_DATE_OF_DEATH, 'yyyyMMdd') as dod, to_date(REG_DATE, 'yyyyMMdd') as reg FROM {collab_database_name}.{project_prefix}{deaths_data}""")

df_death = df_death.withColumn("rule_5", when((col("dod")> col("reg")), 1).otherwise(0))

# COMMAND ----------

df_rule5 = df_death.select(["DEC_CONF_NHS_NUMBER_CLEAN_DEID", "rule_5"]).filter(col("rule_5")==1).dropDuplicates()
#display(df_rule5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule 6: Pregnancy/birth codes for men

# COMMAND ----------

preg = spark.table("global_temp.ccu035_01_pregnancy_birth_sno")
preg = [item.code for item in preg.select('code').collect()]

# COMMAND ----------

gdppr = spark.table(f" {collab_database_name}.{project_prefix}{gdppr_data}")
df_gdppr = gdppr.select(["NHS_NUMBER_DEID", "CODE"]).distinct()
#need to use skinny table's SEX -- bc may have inconsistent SEX in GDPPR
df_skinny_tmp = df_skinny.select(["NHS_NUMBER_DEID", "SEX"])
df_gdppr_sex = df_skinny_tmp.join(
  df_gdppr, 
  ["NHS_NUMBER_DEID"], "inner")

df_gdppr_sex.cache().count() #forces evaluation
#create_table(df_gdppr_sex, table_name="ccu002_vac_df_gdppr_sex")

df_gdppr_preg = df_gdppr_sex.withColumn("rule_6", when((col("CODE").isin(preg)) & (col("SEX")==1), 1).otherwise(0))

# COMMAND ----------

df_rule6 = df_gdppr_preg.select(["NHS_NUMBER_DEID", "rule_6"]).filter(col("rule_6")==1).dropDuplicates()
#display(df_rule6)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule 7: Prostate Cancer Codes for Women

# COMMAND ----------

pros = spark.table("global_temp.ccu035_01_prostate_cancer_sno")
pros = [item.code for item in pros.select('code').collect()]

# COMMAND ----------

df_gdppr = df_gdppr_sex.withColumn("rule_7", when((col("CODE").isin(pros)) & (col("SEX")==2), 1))
df_rule7 = df_gdppr.select(["NHS_NUMBER_DEID", "rule_7"]).filter(col("rule_7")==1).dropDuplicates()
#display(df_rule7)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule 8: Patients have all missing record_dates and dates

# COMMAND ----------

pre_rule_8a_sql = '''CASE WHEN (record_date is not null or record_date not like " " or record_date not like "") or (date is not null or date not like " " or date not like "") THEN 1 ELSE 0 END as complete_record'''
pre_rule_8b_sql = '''CASE WHEN (record_date is null or record_date like " " or record_date like "") and (date is null or date like " " or date like "") THEN 1 ELSE 0 END as null_record'''

# COMMAND ----------

gdppr = spark.table(f" {collab_database_name}.{project_prefix}{gdppr_data}")
df_gdppr = gdppr.selectExpr("*", pre_rule_8a_sql, pre_rule_8b_sql).select(['NHS_NUMBER_DEID', "null_record", "complete_record"])

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import sum as sum_

w = Window.partitionBy('NHS_NUMBER_DEID')
df_gdppr = df_gdppr.withColumn("number_of_complete_records", sum_(df_gdppr.complete_record).over(w)).withColumn("number_of_null_records", sum_(df_gdppr.null_record).over(w))

# COMMAND ----------

df_gdppr = df_gdppr.select(["NHS_NUMBER_DEID", "number_of_null_records", "number_of_complete_records"]).withColumn("rule_8", when((col("number_of_null_records") >0) & (col("number_of_complete_records")==0), 1).otherwise(0))

# COMMAND ----------

df_rule8 = df_gdppr.select(["NHS_NUMBER_DEID", "rule_8"]).filter(col("rule_8")==1).dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract flagged IDs

# COMMAND ----------

rule_cols = [s for s in df_skinny.columns if 'rule_' in s]
rule_cols

# COMMAND ----------

from functools import reduce
df_rule123 = df_skinny.select(["NHS_NUMBER_DEID"]).where(reduce(lambda a, b: a | b, [df_skinny.select(["NHS_NUMBER_DEID"]+ rule_cols)[x] != 0 for x in rule_cols])).distinct()


# COMMAND ----------

df_rule123.cache().count()

# COMMAND ----------

df_rule4 = df_rule4.withColumnRenamed('DEC_CONF_NHS_NUMBER_CLEAN_DEID', 'NHS_NUMBER_DEID')
df_rule5 = df_rule5.withColumnRenamed('DEC_CONF_NHS_NUMBER_CLEAN_DEID', 'NHS_NUMBER_DEID')


# COMMAND ----------

from pyspark.sql import DataFrame
def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)

ids_remove = unionAll(df_rule123.select("NHS_NUMBER_DEID"), 
         df_rule4.select("NHS_NUMBER_DEID"),
         df_rule5.select("NHS_NUMBER_DEID"),
         df_rule6.select("NHS_NUMBER_DEID"),
         df_rule7.select("NHS_NUMBER_DEID"),
         df_rule8.select("NHS_NUMBER_DEID")
        ).distinct()

ids_remove.createOrReplaceGlobalTempView("ids_remove")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save 

# COMMAND ----------

spark.sql(f"""DROP TABLE IF EXISTS {collab_database_name}.{project_prefix}{QA_table_name}""")

# COMMAND ----------

create_table(project_prefix + QA_table_name, select_sql_script=f"SELECT * FROM global_temp.ids_remove") 

# COMMAND ----------



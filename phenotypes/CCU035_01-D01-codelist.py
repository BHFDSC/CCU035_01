# Databricks notebook source
# MAGIC %md # CCU035_01_codelists
# MAGIC 
# MAGIC **Description** This notebook creates global temporary views for the drug codelists needed for the CCU035_01.
# MAGIC 
# MAGIC **Author(s)** Hannah Whittaker
# MAGIC 
# MAGIC **Project(s)** CCU035_01
# MAGIC 
# MAGIC **Reviewer(s)** 
# MAGIC 
# MAGIC **Date last updated** 2022-02-16
# MAGIC 
# MAGIC **Date last reviewed**
# MAGIC 
# MAGIC **Data last run** 
# MAGIC 
# MAGIC **Data input** 
# MAGIC `dars_nic_391419_j3w9t_collab.ccu035_01_primary_care_meds_dars_nic_391419_j3w9t`
# MAGIC 
# MAGIC **Data output** 
# MAGIC 
# MAGIC `global_temp.ccu035_01_drug_codelists`
# MAGIC 
# MAGIC `global_temp.ccu035_01_infection_covariates`
# MAGIC 
# MAGIC `global_temp.ccu035_01_smokingstatus_SNOMED`
# MAGIC 
# MAGIC `global_temp.ccu035_01_pregnancy_birth_sno`
# MAGIC 
# MAGIC `global_temp.cccu035_01_prostate_cancer_sno`
# MAGIC 
# MAGIC `global_temp.ccu035_01_liver_disease`
# MAGIC 
# MAGIC `global_temp.ccu035_01_depression`
# MAGIC 
# MAGIC **Software and versions** SQL, Python
# MAGIC 
# MAGIC **Packages and versions** Not applicable

# COMMAND ----------

# MAGIC %run "/Workspaces/dars_nic_391419_j3w9t_collab/CCU035_01/CCU035_01-functions/wrang000_functions"

# COMMAND ----------

# MAGIC %run "/Workspaces/dars_nic_391419_j3w9t_collab/CCU035_01/CCU035_01-D15-master_notebook_parameters"

# COMMAND ----------

#Data sets parameters
project_prefix = 'ccu035_01_'
collab_database_name = 'dars_nic_391419_j3w9t_collab'
meds_data = 'primary_care_meds_dars_nic_391419_j3w9t'


#Final table parameters
codelist_final_name = 'codelist'


# COMMAND ----------

# MAGIC %md ## Define drug codelists

# COMMAND ----------

#Create global temporary view containing all codelists
spark.sql(f"""
CREATE
OR REPLACE GLOBAL TEMPORARY VIEW ccu035_01_drug_codelists AS
SELECT
  DISTINCT PrescribedBNFCode AS code,
  PrescribedBNFName AS term,
  'BNF' AS system,
  'antiplatelet' AS codelist
FROM
 {collab_database_name}.{project_prefix}{meds_data}
WHERE
  left(PrescribedBNFCode, 4) = '0209'
UNION ALL
SELECT
  DISTINCT PrescribedBNFCode AS code,
  PrescribedBNFName AS term,
  'BNF' AS system,
  'bp_lowering' AS codelist
FROM
  {collab_database_name}.{project_prefix}{meds_data}
WHERE
  left(PrescribedBNFCode, 9) = '0205053A0' -- aliskiren
  OR left(PrescribedBNFCode, 6) = '020504' -- alpha blockers
  OR (
    left(PrescribedBNFCode, 4) = '0204' -- beta blockers
    AND NOT (
      left(PrescribedBNFCode, 9) = '0204000R0' -- exclude propranolol
      OR left(PrescribedBNFCode, 9) = '0204000Q0' -- exclude propranolol
    )
  )
  OR left(PrescribedBNFCode, 6) = '020602' -- calcium channel blockers
  OR (
    left(PrescribedBNFCode, 6) = '020502' -- centrally acting antihypertensives
    AND NOT (
      left(PrescribedBNFCode, 8) = '0205020G' -- guanfacine because it is only used for ADHD
      OR left(PrescribedBNFCode, 9) = '0205052AE' -- drugs for heart failure, not for hypertension
    )
  )
  OR left(PrescribedBNFCode, 6) = '020203' -- potassium sparing diuretics
  OR left(PrescribedBNFCode, 6) = '020201' -- thiazide diuretics
  OR left(PrescribedBNFCode, 6) = '020501' -- vasodilator antihypertensives
  OR left(PrescribedBNFCode, 7) = '0205051' -- angiotensin-converting enzyme inhibitors
  OR left(PrescribedBNFCode, 7) = '0205052' -- angiotensin-II receptor antagonists
UNION ALL
SELECT
  DISTINCT PrescribedBNFCode AS code,
  PrescribedBNFName AS term,
  'BNF' AS system,
  'lipid_lowering' AS codelist
FROM
  {collab_database_name}.{project_prefix}{meds_data}
WHERE
  left(PrescribedBNFCode, 4) = '0212'
UNION ALL
SELECT
  DISTINCT PrescribedBNFCode AS code,
  PrescribedBNFName AS term,
  'BNF' AS system,
  'anticoagulant' AS codelist
FROM
  {collab_database_name}.{project_prefix}{meds_data}
WHERE
  (
    left(PrescribedBNFCode, 6) = '020802'
    AND NOT (
      left(PrescribedBNFCode, 8) = '0208020I'
      OR left(PrescribedBNFCode, 8) = '0208020W'
    )
  )
UNION ALL
SELECT
  DISTINCT PrescribedBNFCode AS code,
  PrescribedBNFName AS term,
  'BNF' AS system,
  'cocp' AS codelist
FROM
  {collab_database_name}.{project_prefix}{meds_data}
WHERE
  left(PrescribedBNFCode, 6) = '070301'
UNION ALL
SELECT
  DISTINCT PrescribedBNFCode AS code,
  PrescribedBNFName AS term,
  'BNF' AS system,
  'hrt' AS codelist
FROM
  {collab_database_name}.{project_prefix}{meds_data}
WHERE
  left(PrescribedBNFCode, 7) = '0604011' """)

# COMMAND ----------

# MAGIC %md ## Define smoking status codelists 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu035_01_smokingstatus_SNOMED  AS
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM VALUES
# MAGIC 
# MAGIC ("160603005","Light cigarette smoker (1-9 cigs/day) (finding)","Current-smoker","Light"),
# MAGIC ("160606002","Very heavy cigarette smoker (40+ cigs/day) (finding)","Current-smoker","Heavy"),
# MAGIC ("160613002","Admitted tobacco consumption possibly untrue (finding)","Current-smoker","Unknown"),
# MAGIC ("160619003","Rolls own cigarettes (finding)","Current-smoker","Unknown"),
# MAGIC ("230056004","Cigarette consumption (observable entity)","Current-smoker","Unknown"),
# MAGIC ("230057008","Cigar consumption (observable entity)","Current-smoker","Unknown"),
# MAGIC ("230058003","Pipe tobacco consumption (observable entity)","Current-smoker","Unknown"),
# MAGIC ("230060001","Light cigarette smoker (finding)","Current-smoker","Light"),
# MAGIC ("230062009","Moderate cigarette smoker (finding)","Current-smoker","Moderate"),
# MAGIC ("230065006","Chain smoker (finding)","Current-smoker","Heavy"),
# MAGIC ("266918002","Tobacco smoking consumption (observable entity)","Current-smoker","Unknown"),
# MAGIC ("446172000","Failed attempt to stop smoking (finding)","Current-smoker","Unknown"),
# MAGIC ("449868002","Smokes tobacco daily (finding)","Current-smoker","Unknown"),
# MAGIC ("56578002","Moderate smoker (20 or less per day) (finding)","Current-smoker","Moderate"),
# MAGIC ("56771006","Heavy smoker (over 20 per day) (finding)","Current-smoker","Heavy"),
# MAGIC ("59978006","Cigar smoker (finding)","Current-smoker","Unknown"),
# MAGIC ("65568007","Cigarette smoker (finding)","Current-smoker","Unknown"),
# MAGIC ("134406006","Smoking reduced (finding)","Current-smoker","Unknown"),
# MAGIC ("160604004","Moderate cigarette smoker (10-19 cigs/day) (finding)","Current-smoker","Moderate"),
# MAGIC ("160605003","Heavy cigarette smoker (20-39 cigs/day) (finding)","Current-smoker","Heavy"),
# MAGIC ("160612007","Keeps trying to stop smoking (finding)","Current-smoker","Unknown"),
# MAGIC ("160616005","Trying to give up smoking (finding)","Current-smoker","Unknown"),
# MAGIC ("203191000000107","Wants to stop smoking (finding)","Current-smoker","Unknown"),
# MAGIC ("225934006","Smokes in bed (finding)","Current-smoker","Unknown"),
# MAGIC ("230059006","Occasional cigarette smoker (finding)","Current-smoker","Light"),
# MAGIC ("230063004","Heavy cigarette smoker (finding)","Current-smoker","Heavy"),
# MAGIC ("230064005","Very heavy cigarette smoker (finding)","Current-smoker","Heavy"),
# MAGIC ("266920004","Trivial cigarette smoker (less than one cigarette/day) (finding)","Current-smoker","Light"),
# MAGIC ("266929003","Smoking started (finding)","Current-smoker","Unknown"),
# MAGIC ("308438006","Smoking restarted (finding)","Current-smoker","Unknown"),
# MAGIC ("394871007","Thinking about stopping smoking (finding)","Current-smoker","Unknown"),
# MAGIC ("394872000","Ready to stop smoking (finding)","Current-smoker","Unknown"),
# MAGIC ("394873005","Not interested in stopping smoking (finding)","Current-smoker","Unknown"),
# MAGIC ("401159003","Reason for restarting smoking (observable entity)","Current-smoker","Unknown"),
# MAGIC ("413173009","Minutes from waking to first tobacco consumption (observable entity)","Current-smoker","Unknown"),
# MAGIC ("428041000124106","Occasional tobacco smoker (finding)","Current-smoker","Light"),
# MAGIC ("77176002","Smoker (finding)","Current-smoker","Unknown"),
# MAGIC ("82302008","Pipe smoker (finding)","Current-smoker","Unknown"),
# MAGIC ("836001000000109","Waterpipe tobacco consumption (observable entity)","Current-smoker","Unknown"),
# MAGIC ("160603005","Light cigarette smoker (1-9 cigs/day) (finding)","Current-smoker","Light"),
# MAGIC ("160612007","Keeps trying to stop smoking (finding)","Current-smoker","Unknown"),
# MAGIC ("160613002","Admitted tobacco consumption possibly untrue (finding)","Current-smoker","Unknown"),
# MAGIC ("160616005","Trying to give up smoking (finding)","Current-smoker","Unknown"),
# MAGIC ("160619003","Rolls own cigarettes (finding)","Current-smoker","Unknown"),
# MAGIC ("160625004","Date ceased smoking (observable entity)","Current-smoker","Unknown"),
# MAGIC ("225934006","Smokes in bed (finding)","Current-smoker","Unknown"),
# MAGIC ("230056004","Cigarette consumption (observable entity)","Current-smoker","Unknown"),
# MAGIC ("230057008","Cigar consumption (observable entity)","Current-smoker","Unknown"),
# MAGIC ("230059006","Occasional cigarette smoker (finding)","Current-smoker","Light"),
# MAGIC ("230060001","Light cigarette smoker (finding)","Current-smoker","Light"),
# MAGIC ("230062009","Moderate cigarette smoker (finding)","Current-smoker","Moderate"),
# MAGIC ("230063004","Heavy cigarette smoker (finding)","Current-smoker","Heavy"),
# MAGIC ("230064005","Very heavy cigarette smoker (finding)","Current-smoker","Heavy"),
# MAGIC ("266920004","Trivial cigarette smoker (less than one cigarette/day) (finding)","Current-smoker","Light"),
# MAGIC ("266929003","Smoking started (finding)","Current-smoker","Unknown"),
# MAGIC ("394872000","Ready to stop smoking (finding)","Current-smoker","Unknown"),
# MAGIC ("401159003","Reason for restarting smoking (observable entity)","Current-smoker","Unknown"),
# MAGIC ("449868002","Smokes tobacco daily (finding)","Current-smoker","Unknown"),
# MAGIC ("65568007","Cigarette smoker (finding)","Current-smoker","Unknown"),
# MAGIC ("134406006","Smoking reduced (finding)","Current-smoker","Unknown"),
# MAGIC ("160604004","Moderate cigarette smoker (10-19 cigs/day) (finding)","Current-smoker","Moderate"),
# MAGIC ("160605003","Heavy cigarette smoker (20-39 cigs/day) (finding)","Current-smoker","Heavy"),
# MAGIC ("160606002","Very heavy cigarette smoker (40+ cigs/day) (finding)","Current-smoker","Heavy"),
# MAGIC ("203191000000107","Wants to stop smoking (finding)","Current-smoker","Unknown"),
# MAGIC ("308438006","Smoking restarted (finding)","Current-smoker","Unknown"),
# MAGIC ("394871007","Thinking about stopping smoking (finding)","Current-smoker","Unknown"),
# MAGIC ("394873005","Not interested in stopping smoking (finding)","Current-smoker","Unknown"),
# MAGIC ("401201003","Cigarette pack-years (observable entity)","Current-smoker","Unknown"),
# MAGIC ("413173009","Minutes from waking to first tobacco consumption (observable entity)","Current-smoker","Unknown"),
# MAGIC ("428041000124106","Occasional tobacco smoker (finding)","Current-smoker","Light"),
# MAGIC ("446172000","Failed attempt to stop smoking (finding)","Current-smoker","Unknown"),
# MAGIC ("56578002","Moderate smoker (20 or less per day) (finding)","Current-smoker","Moderate"),
# MAGIC ("56771006","Heavy smoker (over 20 per day) (finding)","Current-smoker","Heavy"),
# MAGIC ("59978006","Cigar smoker (finding)","Current-smoker","Unknown"),
# MAGIC ("77176002","Smoker (finding)","Current-smoker","Unknown"),
# MAGIC ("82302008","Pipe smoker (finding)","Current-smoker","Unknown"),
# MAGIC ("836001000000109","Waterpipe tobacco consumption (observable entity)","Current-smoker","Unknown"),
# MAGIC ("53896009","Tolerant ex-smoker (finding)","Ex-smoker","Unknown"),
# MAGIC ("1092041000000100","Ex-very heavy smoker (40+/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("1092091000000100","Ex-moderate smoker (10-19/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("160620009","Ex-pipe smoker (finding)","Ex-smoker","Unknown"),
# MAGIC ("160621008","Ex-cigar smoker (finding)","Ex-smoker","Unknown"),
# MAGIC ("228486009","Time since stopped smoking (observable entity)","Ex-smoker","Unknown"),
# MAGIC ("266921000","Ex-trivial cigarette smoker (<1/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("266922007","Ex-light cigarette smoker (1-9/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("266923002","Ex-moderate cigarette smoker (10-19/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("266928006","Ex-cigarette smoker amount unknown (finding)","Ex-smoker","Unknown"),
# MAGIC ("281018007","Ex-cigarette smoker (finding)","Ex-smoker","Unknown"),
# MAGIC ("735128000","Ex-smoker for less than 1 year (finding)","Ex-smoker","Unknown"),
# MAGIC ("8517006","Ex-smoker (finding)","Ex-smoker","Unknown"),
# MAGIC ("1092031000000100","Ex-smoker amount unknown (finding)","Ex-smoker","Unknown"),
# MAGIC ("1092071000000100","Ex-heavy smoker (20-39/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("1092111000000100","Ex-light smoker (1-9/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("1092131000000100","Ex-trivial smoker (<1/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("160617001","Stopped smoking (finding)","Ex-smoker","Unknown"),
# MAGIC ("160625004","Date ceased smoking (observable entity)","Ex-smoker","Unknown"),
# MAGIC ("266924008","Ex-heavy cigarette smoker (20-39/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("266925009","Ex-very heavy cigarette smoker (40+/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("360890004","Intolerant ex-smoker (finding)","Ex-smoker","Unknown"),
# MAGIC ("360900008","Aggressive ex-smoker (finding)","Ex-smoker","Unknown"),
# MAGIC ("48031000119106","Ex-smoker for more than 1 year (finding)","Ex-smoker","Unknown"),
# MAGIC ("492191000000103","Ex roll-up cigarette smoker (finding)","Ex-smoker","Unknown"),
# MAGIC ("53896009","Tolerant ex-smoker (finding)","Ex-smoker","Unknown"),
# MAGIC ("735112005","Date ceased using moist tobacco (observable entity)","Ex-smoker","Unknown"),
# MAGIC ("1092041000000100","Ex-very heavy smoker (40+/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("1092071000000100","Ex-heavy smoker (20-39/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("1092111000000100","Ex-light smoker (1-9/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("228486009","Time since stopped smoking (observable entity)","Ex-smoker","Unknown"),
# MAGIC ("266921000","Ex-trivial cigarette smoker (<1/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("266923002","Ex-moderate cigarette smoker (10-19/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("266928006","Ex-cigarette smoker amount unknown (finding)","Ex-smoker","Unknown"),
# MAGIC ("360900008","Aggressive ex-smoker (finding)","Ex-smoker","Unknown"),
# MAGIC ("492191000000103","Ex roll-up cigarette smoker (finding)","Ex-smoker","Unknown"),
# MAGIC ("735112005","Date ceased using moist tobacco (observable entity)","Ex-smoker","Unknown"),
# MAGIC ("735128000","Ex-smoker for less than 1 year (finding)","Ex-smoker","Unknown"),
# MAGIC ("1092031000000100","Ex-smoker amount unknown (finding)","Ex-smoker","Unknown"),
# MAGIC ("1092091000000100","Ex-moderate smoker (10-19/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("1092131000000100","Ex-trivial smoker (<1/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("160617001","Stopped smoking (finding)","Ex-smoker","Unknown"),
# MAGIC ("160620009","Ex-pipe smoker (finding)","Ex-smoker","Unknown"),
# MAGIC ("160621008","Ex-cigar smoker (finding)","Ex-smoker","Unknown"),
# MAGIC ("230058003","Pipe tobacco consumption (observable entity)","Ex-smoker","Unknown"),
# MAGIC ("230065006","Chain smoker (finding)","Ex-smoker","Unknown"),
# MAGIC ("266918002","Tobacco smoking consumption (observable entity)","Ex-smoker","Unknown"),
# MAGIC ("266922007","Ex-light cigarette smoker (1-9/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("266924008","Ex-heavy cigarette smoker (20-39/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("266925009","Ex-very heavy cigarette smoker (40+/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("281018007","Ex-cigarette smoker (finding)","Ex-smoker","Unknown"),
# MAGIC ("360890004","Intolerant ex-smoker (finding)","Ex-smoker","Unknown"),
# MAGIC ("48031000119106","Ex-smoker for more than 1 year (finding)","Ex-smoker","Unknown"),
# MAGIC ("8517006","Ex-smoker (finding)","Ex-smoker","Unknown"),
# MAGIC ("221000119102","Never smoked any substance (finding)","Never-smoker","NA"),
# MAGIC ("266919005","Never smoked tobacco (finding)","Never-smoker","NA"),
# MAGIC ("221000119102","Never smoked any substance (finding)","Never-smoker","NA"),
# MAGIC ("266919005","Never smoked tobacco (finding)","Never-smoker","NA")
# MAGIC 
# MAGIC AS tab(conceptID, description, smoking_status, severity);

# COMMAND ----------

# MAGIC %md ## Pregnancy & birth codelist

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMPORARY VIEW ccu035_01_pregnancy_birth_sno AS
# MAGIC SELECT *
# MAGIC FROM VALUES
# MAGIC ("171057006","Pregnancy alcohol education (procedure)"),
# MAGIC ("72301000119103","Asthma in pregnancy (disorder)"),
# MAGIC ("10742121000119104","Asthma in mother complicating childbirth (disorder)"),
# MAGIC ("10745291000119103","Malignant neoplastic disease in mother complicating childbirth (disorder)"),
# MAGIC ("10749871000119100","Malignant neoplastic disease in pregnancy (disorder)"),
# MAGIC ("20753005","Hypertensive heart disease complicating AND/OR reason for care during pregnancy (disorder)"),
# MAGIC ("237227006","Congenital heart disease in pregnancy (disorder)"),
# MAGIC ("169501005","Pregnant, diaphragm failure (finding)"),
# MAGIC ("169560008","Pregnant - urine test confirms (finding)"),
# MAGIC ("169561007","Pregnant - blood test confirms (finding)"),
# MAGIC ("169562000","Pregnant - vaginal examination confirms (finding)"),
# MAGIC ("169565003","Pregnant - planned (finding)"),
# MAGIC ("169566002","Pregnant - unplanned - wanted (finding)"),
# MAGIC ("413567003","Aplastic anemia associated with pregnancy (disorder)"),
# MAGIC ("91948008","Asymptomatic human immunodeficiency virus infection in pregnancy (disorder)"),
# MAGIC ("169488004","Contraceptive intrauterine device failure - pregnant (finding)"),
# MAGIC ("169508004","Pregnant, sheath failure (finding)"),
# MAGIC ("169564004","Pregnant - on abdominal palpation (finding)"),
# MAGIC ("77386006","Pregnant (finding)"),
# MAGIC ("10746341000119109","Acquired immune deficiency syndrome complicating childbirth (disorder)"),
# MAGIC ("10759351000119103","Sickle cell anemia in mother complicating childbirth (disorder)"),
# MAGIC ("10757401000119104","Pre-existing hypertensive heart and chronic kidney disease in mother complicating childbirth (disorder)"),
# MAGIC ("10757481000119107","Pre-existing hypertensive heart and chronic kidney disease in mother complicating pregnancy (disorder)"),
# MAGIC ("10757441000119102","Pre-existing hypertensive heart disease in mother complicating childbirth (disorder)"),
# MAGIC ("10759031000119106","Pre-existing hypertensive heart disease in mother complicating pregnancy (disorder)"),
# MAGIC ("1474004","Hypertensive heart AND renal disease complicating AND/OR reason for care during childbirth (disorder)"),
# MAGIC ("199006004","Pre-existing hypertensive heart disease complicating pregnancy, childbirth and the puerperium (disorder)"),
# MAGIC ("199007008","Pre-existing hypertensive heart and renal disease complicating pregnancy, childbirth and the puerperium (disorder)"),
# MAGIC ("22966008","Hypertensive heart AND renal disease complicating AND/OR reason for care during pregnancy (disorder)"),
# MAGIC ("59733002","Hypertensive heart disease complicating AND/OR reason for care during childbirth (disorder)"),
# MAGIC ("171054004","Pregnancy diet education (procedure)"),
# MAGIC ("106281000119103","Pre-existing diabetes mellitus in mother complicating childbirth (disorder)"),
# MAGIC ("10754881000119104","Diabetes mellitus in mother complicating childbirth (disorder)"),
# MAGIC ("199225007","Diabetes mellitus during pregnancy - baby delivered (disorder)"),
# MAGIC ("237627000","Pregnancy and type 2 diabetes mellitus (disorder)"),
# MAGIC ("609563008","Pre-existing diabetes mellitus in pregnancy (disorder)"),
# MAGIC ("609566000","Pregnancy and type 1 diabetes mellitus (disorder)"),
# MAGIC ("609567009","Pre-existing type 2 diabetes mellitus in pregnancy (disorder)"),
# MAGIC ("199223000","Diabetes mellitus during pregnancy, childbirth and the puerperium (disorder)"),
# MAGIC ("199227004","Diabetes mellitus during pregnancy - baby not yet delivered (disorder)"),
# MAGIC ("609564002","Pre-existing type 1 diabetes mellitus in pregnancy (disorder)"),
# MAGIC ("76751001","Diabetes mellitus in mother complicating pregnancy, childbirth AND/OR puerperium (disorder)"),
# MAGIC ("526961000000105","Pregnancy advice for patients with epilepsy (procedure)"),
# MAGIC ("527041000000108","Pregnancy advice for patients with epilepsy not indicated (situation)"),
# MAGIC ("527131000000100","Pregnancy advice for patients with epilepsy declined (situation)"),
# MAGIC ("10753491000119101","Gestational diabetes mellitus in childbirth (disorder)"),
# MAGIC ("40801000119106","Gestational diabetes mellitus complicating pregnancy (disorder)"),
# MAGIC ("10562009","Malignant hypertension complicating AND/OR reason for care during childbirth (disorder)"),
# MAGIC ("198944004","Benign essential hypertension complicating pregnancy, childbirth and the puerperium - delivered (disorder)"),
# MAGIC ("198945003","Benign essential hypertension complicating pregnancy, childbirth and the puerperium - delivered with postnatal complication (disorder)"),
# MAGIC ("198946002","Benign essential hypertension complicating pregnancy, childbirth and the puerperium - not delivered (disorder)"),
# MAGIC ("198949009","Renal hypertension complicating pregnancy, childbirth and the puerperium (disorder)"),
# MAGIC ("198951008","Renal hypertension complicating pregnancy, childbirth and the puerperium - delivered (disorder)"),
# MAGIC ("198954000","Renal hypertension complicating pregnancy, childbirth and the puerperium with postnatal complication (disorder)"),
# MAGIC ("199005000","Pre-existing hypertension complicating pregnancy, childbirth and puerperium (disorder)"),
# MAGIC ("23717007","Benign essential hypertension complicating AND/OR reason for care during pregnancy (disorder)"),
# MAGIC ("26078007","Hypertension secondary to renal disease complicating AND/OR reason for care during childbirth (disorder)"),
# MAGIC ("29259002","Malignant hypertension complicating AND/OR reason for care during pregnancy (disorder)"),
# MAGIC ("65402008","Pre-existing hypertension complicating AND/OR reason for care during pregnancy (disorder)"),
# MAGIC ("8218002","Chronic hypertension complicating AND/OR reason for care during childbirth (disorder)"),
# MAGIC ("10752641000119102","Eclampsia with pre-existing hypertension in childbirth (disorder)"),
# MAGIC ("118781000119108","Pre-existing hypertensive chronic kidney disease in mother complicating pregnancy (disorder)"),
# MAGIC ("18416000","Essential hypertension complicating AND/OR reason for care during childbirth (disorder)"),
# MAGIC ("198942000","Benign essential hypertension complicating pregnancy, childbirth and the puerperium (disorder)"),
# MAGIC ("198947006","Benign essential hypertension complicating pregnancy, childbirth and the puerperium with postnatal complication (disorder)"),
# MAGIC ("198952001","Renal hypertension complicating pregnancy, childbirth and the puerperium - delivered with postnatal complication (disorder)"),
# MAGIC ("198953006","Renal hypertension complicating pregnancy, childbirth and the puerperium - not delivered (disorder)"),
# MAGIC ("199008003","Pre-existing secondary hypertension complicating pregnancy, childbirth and puerperium (disorder)"),
# MAGIC ("34694006","Pre-existing hypertension complicating AND/OR reason for care during childbirth (disorder)"),
# MAGIC ("37618003","Chronic hypertension complicating AND/OR reason for care during pregnancy (disorder)"),
# MAGIC ("48552006","Hypertension secondary to renal disease complicating AND/OR reason for care during pregnancy (disorder)"),
# MAGIC ("71874008","Benign essential hypertension complicating AND/OR reason for care during childbirth (disorder)"),
# MAGIC ("78808002","Essential hypertension complicating AND/OR reason for care during pregnancy (disorder)"),
# MAGIC ("91923005","Acquired immunodeficiency syndrome virus infection associated with pregnancy (disorder)"),
# MAGIC ("10755671000119100","Human immunodeficiency virus in mother complicating childbirth (disorder)"),
# MAGIC ("721166000","Human immunodeficiency virus complicating pregnancy childbirth and the puerperium (disorder)"),
# MAGIC ("449369001","Stopped smoking before pregnancy (finding)"),
# MAGIC ("449345000","Smoked before confirmation of pregnancy (finding)"),
# MAGIC ("449368009","Stopped smoking during pregnancy (finding)"),
# MAGIC ("88144003","Removal of ectopic interstitial uterine pregnancy requiring total hysterectomy (procedure)"),
# MAGIC ("240154002","Idiopathic osteoporosis in pregnancy (disorder)"),
# MAGIC ("956951000000104","Pertussis vaccination in pregnancy (procedure)"),
# MAGIC ("866641000000105","Pertussis vaccination in pregnancy declined (situation)"),
# MAGIC ("956971000000108","Pertussis vaccination in pregnancy given by other healthcare provider (finding)"),
# MAGIC ("169563005","Pregnant - on history (finding)"),
# MAGIC ("10231000132102","In-vitro fertilization pregnancy (finding)"),
# MAGIC ("134781000119106","High risk pregnancy due to recurrent miscarriage (finding)"),
# MAGIC ("16356006","Multiple pregnancy (disorder)"),
# MAGIC ("237239003","Low risk pregnancy (finding)"),
# MAGIC ("276367008","Wanted pregnancy (finding)"),
# MAGIC ("314204000","Early stage of pregnancy (finding)"),
# MAGIC ("439311009","Intends to continue pregnancy (finding)"),
# MAGIC ("713575004","Dizygotic twin pregnancy (disorder)"),
# MAGIC ("80997009","Quintuplet pregnancy (disorder)"),
# MAGIC ("1109951000000101","Pregnancy insufficiently advanced for reliable antenatal screening (finding)"),
# MAGIC ("1109971000000105","Pregnancy too advanced for reliable antenatal screening (finding)"),
# MAGIC ("237238006","Pregnancy with uncertain dates (finding)"),
# MAGIC ("444661007","High risk pregnancy due to history of preterm labor (finding)"),
# MAGIC ("459166009","Dichorionic diamniotic twin pregnancy (disorder)"),
# MAGIC ("459167000","Monochorionic twin pregnancy (disorder)"),
# MAGIC ("459168005","Monochorionic diamniotic twin pregnancy (disorder)"),
# MAGIC ("459171002","Monochorionic monoamniotic twin pregnancy (disorder)"),
# MAGIC ("47200007","High risk pregnancy (finding)"),
# MAGIC ("60810003","Quadruplet pregnancy (disorder)"),
# MAGIC ("64254006","Triplet pregnancy (disorder)"),
# MAGIC ("65147003","Twin pregnancy (disorder)"),
# MAGIC ("713576003","Monozygotic twin pregnancy (disorder)"),
# MAGIC ("171055003","Pregnancy smoking education (procedure)"),
# MAGIC ("10809101000119109","Hypothyroidism in childbirth (disorder)"),
# MAGIC ("428165003","Hypothyroidism in pregnancy (disorder)")
# MAGIC 
# MAGIC AS tab(code, term)

# COMMAND ----------

# MAGIC %md ## Prostate cancer codelist

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMPORARY VIEW ccu035_01_prostate_cancer_sno AS
# MAGIC SELECT *
# MAGIC FROM VALUES
# MAGIC ("126906006","Neoplasm of prostate"),
# MAGIC ("81232004","Radical cystoprostatectomy"),
# MAGIC ("176106009","Radical cystoprostatourethrectomy"),
# MAGIC ("176261008","Radical prostatectomy without pelvic node excision"),
# MAGIC ("176262001","Radical prostatectomy with pelvic node sampling"),
# MAGIC ("176263006","Radical prostatectomy with pelvic lymphadenectomy"),
# MAGIC ("369775001","Gleason Score 2-4: Well differentiated"),
# MAGIC ("369777009","Gleason Score 8-10: Poorly differentiated"),
# MAGIC ("385377005","Gleason grade finding for prostatic cancer (finding)"),
# MAGIC ("394932008","Gleason prostate grade 5-7 (medium) (finding)"),
# MAGIC ("399068003","Malignant tumor of prostate (disorder)"),
# MAGIC ("428262008","History of malignant neoplasm of prostate (situation)")
# MAGIC 
# MAGIC AS tab(code, term)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Depression codelist

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view ccu035_01_depression as 
# MAGIC select *
# MAGIC from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
# MAGIC where name like 'depression'
# MAGIC and 
# MAGIC (terminology like 'SNOMED'
# MAGIC or terminology like 'ICD10')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Obesity codelist

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temporary view ccu035_01_BMI_obesity as
# MAGIC select *
# MAGIC from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
# MAGIC where name like '%BMI_obesity'
# MAGIC and 
# MAGIC (terminology like 'SNOMED'
# MAGIC or terminology like 'ICD10')
# MAGIC union all
# MAGIC SELECT *
# MAGIC FROM VALUES
# MAGIC ('BMI_obesity','ICD10','E66','Diagnosis of obesity','','')
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hypertension

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temporary view ccu035_01_hypertension as
# MAGIC select *
# MAGIC from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
# MAGIC where name like '%hypertension%'
# MAGIC and 
# MAGIC (terminology like 'SNOMED'
# MAGIC or terminology like 'ICD10'
# MAGIC or terminology like 'DMD')
# MAGIC union all
# MAGIC select *
# MAGIC from values
# MAGIC ('hypertension','ICD10','I10','Essential (primary) hypertension','',''),
# MAGIC ('hypertension','ICD10','I11','Hypertensive heart disease','',''),
# MAGIC ('hypertension','ICD10','I12','Hypertensive renal disease','',''),
# MAGIC ('hypertension','ICD10','I13','Hypertensive heart and renal disease','',''),
# MAGIC ('hypertension','ICD10','I15','Secondary hypertension','','')
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diabetes

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temporary view ccu035_01_diabetes as
# MAGIC select *
# MAGIC from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
# MAGIC where name like '%diabetes%'
# MAGIC and 
# MAGIC (terminology like 'SNOMED'
# MAGIC or terminology like 'ICD10'
# MAGIC or terminology like 'DMD')
# MAGIC union all
# MAGIC select *
# MAGIC from values
# MAGIC ('diabetes','ICD10','E10','Insulin-dependent diabetes mellitus','',''),
# MAGIC ('diabetes','ICD10','E11','Non-insulin-dependent diabetes mellitus','',''),
# MAGIC ('diabetes','ICD10','E12','Malnutrition-related diabetes mellitus','',''),
# MAGIC ('diabetes','ICD10','O242','Diabetes mellitus in pregnancy: Pre-existing malnutrition-related diabetes mellitus','',''),
# MAGIC ('diabetes','ICD10','E13','Other specified diabetes mellitus','',''),
# MAGIC ('diabetes','ICD10','E14','Unspecified diabetes mellitus','',''),
# MAGIC ('diabetes','ICD10','G590','Diabetic mononeuropathy','',''),
# MAGIC ('diabetes','ICD10','G632','Diabetic polyneuropathy','',''),
# MAGIC ('diabetes','ICD10','H280','Diabetic cataract','',''),
# MAGIC ('diabetes','ICD10','H360','Diabetic retinopathy','',''),
# MAGIC ('diabetes','ICD10','M142','Diabetic arthropathy','',''),
# MAGIC ('diabetes','ICD10','N083','Glomerular disorders in diabetes mellitus','',''),
# MAGIC ('diabetes','ICD10','O240','Diabetes mellitus in pregnancy: Pre-existing diabetes mellitus, insulin-dependent','',''),
# MAGIC ('diabetes','ICD10','O241','Diabetes mellitus in pregnancy: Pre-existing diabetes mellitus, non-insulin-dependent','',''),
# MAGIC ('diabetes','ICD10','O243','Diabetes mellitus in pregnancy: Pre-existing diabetes mellitus, unspecified','','')
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cancer

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temporary view ccu035_01_cancer as
# MAGIC select *
# MAGIC from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
# MAGIC where name like '%cancer%'
# MAGIC and 
# MAGIC (terminology like 'SNOMED'
# MAGIC or terminology like 'ICD10')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Liver disease

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu035_01_liver AS
# MAGIC select *
# MAGIC from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
# MAGIC where name like '%liver%'
# MAGIC and 
# MAGIC terminology like 'ICD10'
# MAGIC union all
# MAGIC SELECT
# MAGIC 	pheno.name, 
# MAGIC     CONCAT(pheno.terminology, '_SNOMEDmapped') AS terminology,
# MAGIC 	pheno.term AS term, 
# MAGIC 	mapfile.SCT_CONCEPTID AS code,
# MAGIC     "" AS code_type,
# MAGIC   "" AS RecordDate
# MAGIC FROM
# MAGIC 	bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127 AS pheno,
# MAGIC 	dss_corporate.read_codes_map_ctv3_to_snomed AS mapfile 
# MAGIC WHERE pheno.name LIKE  "%liver%" 
# MAGIC AND pheno.terminology ="CTV3"
# MAGIC AND pheno.code = mapfile.CTV3_CONCEPTID 
# MAGIC AND mapfile.IS_ASSURED = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dementia

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu035_01_dementia AS
# MAGIC select *
# MAGIC from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
# MAGIC where name like 'dementia%'
# MAGIC and 
# MAGIC (terminology like 'ICD10'
# MAGIC or terminology like 'SNOMED')

# COMMAND ----------

# MAGIC %md
# MAGIC ### CKD

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu035_01_ckd AS
# MAGIC select *
# MAGIC from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
# MAGIC where name like 'CKD%'
# MAGIC and 
# MAGIC (terminology like 'ICD10'
# MAGIC or terminology like 'SNOMED')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Myocardial infarction

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu035_01_ami AS
# MAGIC select terminology, code, term, code_type, recorddate,
# MAGIC case 
# MAGIC when code like 'I25.2' or code like 'I24.1' then 'AMI_covariate_only'
# MAGIC else
# MAGIC 'AMI'
# MAGIC end as new_name
# MAGIC from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
# MAGIC where name like 'AMI%'
# MAGIC and 
# MAGIC (terminology like 'ICD10'
# MAGIC or terminology like 'SNOMED')

# COMMAND ----------

# MAGIC %md
# MAGIC ### VT

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace global temp view ccu035_01_vt as
# MAGIC 
# MAGIC select * 
# MAGIC from values
# MAGIC ("DVT_DVT","ICD10","I80","Phlebitis and thrombophlebitis","1","20210127"),
# MAGIC ("other_DVT","ICD10","I82.8","Other vein thrombosis","1","20210127"),
# MAGIC ("other_DVT","ICD10","I82.9","Other vein thrombosis","1","20210127"),
# MAGIC ("other_DVT","ICD10","I82.0","Other vein thrombosis","1","20210127"),
# MAGIC ("other_DVT","ICD10","I82.3","Other vein thrombosis","1","20210127"),
# MAGIC ("other_DVT","ICD10","I82.2","Other vein thrombosis","1","20210127"),
# MAGIC ("DVT_pregnancy","ICD10","O22.3","Thrombosis during pregnancy and puerperium","1","20210127"),
# MAGIC ("DVT_pregnancy","ICD10","O87.1","Thrombosis during pregnancy and puerperium","1","20210127"),
# MAGIC ("DVT_pregnancy","ICD10","O87.9","Thrombosis during pregnancy and puerperium","1","20210127"),
# MAGIC ("DVT_pregnancy","ICD10","O88.2","Thrombosis during pregnancy and puerperium","1","20210127"),
# MAGIC ("ICVT_pregnancy","ICD10","O22.5","ntracranial venous thrombosis in pregnancy and puerperium","1","20210127"),
# MAGIC ("ICVT_pregnancy","ICD10","O87.3","ntracranial venous thrombosis in pregnancy and puerperium","1","20210127"),
# MAGIC ("portal_vein_thrombosis","ICD10","I81","Portal vein thrombosis","1","20210127"),
# MAGIC ("VT_covariate_only","ICD10","O08.2","Embolism following abortion and ectopic and molar pregnancy","1","20210127")
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate);

# COMMAND ----------

# MAGIC %md
# MAGIC ### DVT_ICVT

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace global temp view ccu035_01_DVT_ICVT as
# MAGIC 
# MAGIC select * 
# MAGIC from values
# MAGIC ("DVT_ICVT","ICD10","G08","Intracranial venous thrombosis","1","20210127"),
# MAGIC ("DVT_ICVT","ICD10","I67.6","Intracranial venous thrombosis","1","20210127"),
# MAGIC ("DVT_ICVT","ICD10","I63.6","Intracranial venous thrombosis","1","20210127"),
# MAGIC ("DVT_ICVT_covariate_only","SNOMED","195230003","Cerebral infarction due to cerebral venous thrombosis,  nonpyogenic","1","20210127")
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate);

# COMMAND ----------

# MAGIC %md
# MAGIC ### PE

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace global temp view ccu035_01_PE as
# MAGIC 
# MAGIC select * 
# MAGIC from values
# MAGIC 
# MAGIC ("PE","ICD10","I26.0","Pulmonary embolism","1","20210127"),
# MAGIC ("PE","ICD10","I26.9","Pulmonary embolism","1","20210127"),
# MAGIC ("PE_covariate_only","SNOMED","438773007","Pulmonary embolism with mention of acute cor pulmonale","1","20210127"),
# MAGIC ("PE_covariate_only","SNOMED","133971000119108","Pulmonary embolism with mention of acute cor pulmonale","1","20210127")
# MAGIC 
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ischemic stroke 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace global temp view ccu035_01_stroke_IS as
# MAGIC 
# MAGIC with cte as
# MAGIC (select terminology, code, term, code_type, RecordDate,
# MAGIC case 
# MAGIC when terminology like 'SNOMED' then 'stroke_isch'
# MAGIC end as new_name
# MAGIC from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
# MAGIC where name like 'stroke_IS'
# MAGIC AND terminology ='SNOMED'
# MAGIC AND code_type='1' 
# MAGIC AND RecordDate='20210127'
# MAGIC )
# MAGIC 
# MAGIC select * 
# MAGIC from values
# MAGIC ("stroke_isch","ICD10","I63.0","Cerebral infarction","1","20210127"),
# MAGIC ("stroke_isch","ICD10","I63.1","Cerebral infarction","1","20210127"),
# MAGIC ("stroke_isch","ICD10","I63.2","Cerebral infarction","1","20210127"),
# MAGIC ("stroke_isch","ICD10","I63.3","Cerebral infarction","1","20210127"),
# MAGIC ("stroke_isch","ICD10","I63.4","Cerebral infarction","1","20210127"),
# MAGIC ("stroke_isch","ICD10","I63.5","Cerebral infarction","1","20210127"),
# MAGIC ("stroke_isch","ICD10","I63.8","Cerebral infarction","1","20210127"),
# MAGIC ("stroke_isch","ICD10","I63.9","Cerebral infarction","1","20210127")
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate)
# MAGIC union all
# MAGIC select new_name as name, terminology, code, term, code_type, RecordDate
# MAGIC from cte

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stroke, NOS 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu035_01_stroke_NOS AS
# MAGIC 
# MAGIC select *,
# MAGIC case 
# MAGIC when name like 'stroke_NOS%' then 'stroke_isch'
# MAGIC end as new_name
# MAGIC from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
# MAGIC where name like 'stroke_NOS%'
# MAGIC and 
# MAGIC (terminology like 'ICD10'
# MAGIC or terminology like 'SNOMED')
# MAGIC and term != 'Stroke in the puerperium (disorder)'
# MAGIC and term != 'Cerebrovascular accident (disorder)'
# MAGIC and term != 'Right sided cerebral hemisphere cerebrovascular accident (disorder)'
# MAGIC and term != 'Left sided cerebral hemisphere cerebrovascular accident (disorder)'
# MAGIC and term != 'Brainstem stroke syndrome (disorder)'
# MAGIC AND code_type='1' 
# MAGIC AND RecordDate='20210127'

# COMMAND ----------

# MAGIC %md
# MAGIC ### stroke_SAH

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu035_01_stroke_SAH AS
# MAGIC select terminology, code, term, code_type, recorddate,
# MAGIC case 
# MAGIC when terminology like 'ICD10' then 'stroke_SAH_HS'
# MAGIC end as new_name
# MAGIC from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
# MAGIC where name like 'stroke_SAH%'
# MAGIC and 
# MAGIC terminology like 'ICD10'

# COMMAND ----------

# MAGIC %md
# MAGIC ### stroke_HS

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu035_01_stroke_HS AS
# MAGIC select terminology, code, term, code_type, recorddate,
# MAGIC case 
# MAGIC when terminology like 'SNOMED' then 'stroke_SAH_HS_covariate_only'
# MAGIC else 'stroke_SAH_HS'
# MAGIC end as new_name
# MAGIC from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
# MAGIC where name like 'stroke_HS%'
# MAGIC and 
# MAGIC (terminology like 'ICD10'
# MAGIC or terminology like 'SNOMED')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Thrombophilia

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace global temp view ccu035_01_thrombophilia as
# MAGIC 
# MAGIC select * 
# MAGIC from values
# MAGIC ("thrombophilia","ICD10","D68.5","Primary thrombophilia","1","20210127"),
# MAGIC ("thrombophilia","ICD10","D68.6","Other thrombophilia","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","439001009","Acquired thrombophilia","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","441882000","History of thrombophilia","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","439698008","Primary thrombophilia","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","234467004","Thrombophilia","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","441697004","Thrombophilia associated with pregnancy","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","442760001","Thrombophilia caused by antineoplastic agent therapy","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","442197003","Thrombophilia caused by drug therapy","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","442654007","Thrombophilia caused by hormone therapy","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","442363001","Thrombophilia caused by vascular device","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","439126002","Thrombophilia due to acquired antithrombin III deficiency","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","439002002","Thrombophilia due to acquired protein C deficiency","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","439125003","Thrombophilia due to acquired protein S deficiency","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","441079006","Thrombophilia due to antiphospholipid antibody","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","441762006","Thrombophilia due to immobilisation","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","442078001","Thrombophilia due to malignant neoplasm","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","441946009","Thrombophilia due to myeloproliferative disorder","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","441990004","Thrombophilia due to paroxysmal nocturnal haemoglobinuria","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","441945008","Thrombophilia due to trauma","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","442121006","Thrombophilia due to vascular anomaly","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","439698008","Hereditary thrombophilia","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","783250007","Hereditary thrombophilia due to congenital histidine-rich (poly-L) glycoprotein deficiency","1",	"20210127")
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Thrombocytopenia

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace global temp view ccu035_01_TCP as
# MAGIC 
# MAGIC select * 
# MAGIC from values
# MAGIC ("thrombocytopenia","ICD10","D69.3","Thrombocytopenia","1","20210127"),
# MAGIC ("thrombocytopenia","ICD10","D69.4","Thrombocytopenia","1","20210127"),
# MAGIC ("thrombocytopenia","ICD10","D69.5","Thrombocytopenia","1","20210127"),
# MAGIC ("thrombocytopenia","ICD10","D69.6","Thrombocytopenia","1","20210127"),
# MAGIC ("TTP","ICD10","M31.1","Thrombotic microangiopathy","1","20210127"),
# MAGIC ("TCP_covariate_only","SNOMED","74576004","Acquired thrombocytopenia","1","20210127"),
# MAGIC ("TCP_covariate_only","SNOMED","439007008","Acquired thrombotic thrombocytopenic purpura","1","20210127"),
# MAGIC ("TCP_covariate_only","SNOMED","28505005","Acute idiopathic thrombocytopenic purpura","1","20210127"),
# MAGIC ("TCP_covariate_only","SNOMED","128091003","Autoimmune thrombocytopenia","1","20210127"),
# MAGIC ("TCP_covariate_only","SNOMED","13172003","Autoimmune thrombocytopenic purpura","1","20210127"),
# MAGIC ("TCP_covariate_only","SNOMED","438476003","Autoimmune thrombotic thrombocytopenic purpura","1","20210127"),
# MAGIC ("TCP_covariate_only","SNOMED","111588002","Heparin associated thrombotic thrombocytopenia","1","20210127"),
# MAGIC ("TCP_covariate_only","SNOMED","73397007","Heparin induced thrombocytopaenia","1","20210127"),
# MAGIC ("TCP_covariate_only","SNOMED","438492008","Hereditary thrombocytopenic disorder","1","20210127"),
# MAGIC ("TCP_covariate_only","SNOMED","441511006","History of immune thrombocytopenia","1","20210127"),
# MAGIC ("TCP_covariate_only","SNOMED","49341000119108","History of thrombocytopaenia",	"1","20210127"),
# MAGIC ("TCP_covariate_only","SNOMED","726769004","HIT (Heparin induced thrombocytopenia) antibody","1","20210127"),
# MAGIC ("TCP_covariate_only","SNOMED","371106008","Idiopathic maternal thrombocytopenia","1","20210127"),
# MAGIC ("TCP_covariate_only","SNOMED","32273002","Idiopathic thrombocytopenic purpura","1","20210127"),
# MAGIC ("TCP_covariate_only","SNOMED","2897005","Immune thrombocytopenia","1","20210127"),
# MAGIC ("TCP_covariate_only","SNOMED","32273002","Immune thrombocytopenic purpura","1","20210127"),
# MAGIC ("TCP_covariate_only","SNOMED","36070007","Immunodeficiency with thrombocytopenia AND eczema","1","20210127"),
# MAGIC ("TCP_covariate_only","SNOMED","33183004","Post infectious thrombocytopenic purpura","1","20210127"),
# MAGIC ("TCP_covariate_only","SNOMED","267534000","Primary thrombocytopenia","1","20210127"),
# MAGIC ("TCP_covariate_only","SNOMED","154826009","Secondary thrombocytopenia","1","20210127"),
# MAGIC ("TCP_covariate_only","SNOMED","866152006","Thrombocytopenia due to 2019 novel coronavirus","1","20210127"),
# MAGIC ("TCP_covariate_only","SNOMED","82190001","Thrombocytopenia due to defective platelet production","1","20210127"),
# MAGIC ("TCP_covariate_only","SNOMED","78345002","Thrombocytopenia due to diminished platelet production","1","20210127"),
# MAGIC ("TCP_covariate_only","SNOMED","191323001","Thrombocytopenia due to extracorporeal circulation of blood","1","20210127"),
# MAGIC ("TCP_covariate_only","SNOMED","87902006",	"Thrombocytopenia due to non-immune destruction","1","20210127"),
# MAGIC ("TCP_covariate_only","SNOMED","302873008","Thrombocytopenic purpura",	"1","20210127"),
# MAGIC ("TCP_covariate_only","SNOMED","417626001","Thrombocytopenic purpura associated with metabolic disorder","1","20210127"),
# MAGIC ("TCP_covariate_only","SNOMED","402653004","Thrombocytopenic purpura due to defective platelet production","1","20210127"),
# MAGIC ("TCP_covariate_only","SNOMED","402654005","Thrombocytopenic purpura due to platelet consumption","1","20210127"),
# MAGIC ("TCP_covariate_only","SNOMED","78129009","Thrombotic thrombocytopenic purpura","1","20210127"),
# MAGIC ("TCP_covariate_only","SNOMED","441322009","Drug induced thrombotic thrombocytopenic purpura","1","20210127"),
# MAGIC ("TCP_covariate_only","SNOMED","19307009","Drug-induced immune thrombocytopenia","1","20210127"),
# MAGIC ("TCP_covariate_only","SNOMED","783251006","Hereditary thrombocytopenia with normal platelets","1","20210127"),
# MAGIC ("TCP_covariate_only","SNOMED","191322006","Thrombocytopenia caused by drugs","1","20210127")
# MAGIC 
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Retinal Infarction

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view ccu035_01_retinal_infarction as
# MAGIC select *
# MAGIC from values
# MAGIC ("stroke_isch","ICD10","H34","Retinal vascular occlusions","1","20210127")
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Other arterial embolism

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view ccu035_01_arterial_embolism as
# MAGIC select *
# MAGIC from values
# MAGIC ("other_arterial_embolism","ICD10","I74","arterial embolism and thrombosis","1","20210127")
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Disseminated intravascular coagulation (DIC)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view ccu035_01_DIC as
# MAGIC select *
# MAGIC from values
# MAGIC ("DIC","ICD10","D65","Disseminated intravascular coagulation","1","20210127")
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mesenteric thrombus

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view ccu035_01_mesenteric_thrombus as
# MAGIC select *
# MAGIC from values
# MAGIC ("mesenteric_thrombus","ICD10","K55.9","Acute vascular disorders of intestine","1","20210127"),
# MAGIC ("mesenteric_thrombus","ICD10","K55.0","Acute vascular disorders of intestine","1","20210127")
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spinal stroke

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view ccu035_01_spinal_stroke as
# MAGIC select *
# MAGIC from values
# MAGIC ("stroke_isch","ICD10","G95.1","Avascular myelopathies (arterial or venous)","1","20210127")
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view ccu035_01_fracture as
# MAGIC select *
# MAGIC from values
# MAGIC 
# MAGIC ("fracture","ICD10","S720","Upper leg fracture","",""),
# MAGIC ("fracture","ICD10","S721","Upper leg fracture","",""),
# MAGIC ("fracture","ICD10","S723","Upper leg fracture","",""),
# MAGIC ("fracture","ICD10","S724","Upper leg fracture","",""),
# MAGIC ("fracture","ICD10","S727","Upper leg fracture","",""),
# MAGIC ("fracture","ICD10","S728","Upper leg fracture","",""),
# MAGIC ("fracture","ICD10","S729","Upper leg fracture","",""),
# MAGIC ("fracture","ICD10","S820","Lower leg fracture","",""),
# MAGIC ("fracture","ICD10","S821","Lower leg fracture","",""),
# MAGIC ("fracture","ICD10","S822","Lower leg fracture","",""),
# MAGIC ("fracture","ICD10","S823","Lower leg fracture","",""),
# MAGIC ("fracture","ICD10","S824","Lower leg fracture","",""),
# MAGIC ("fracture","ICD10","S825","Lower leg fracture","",""),
# MAGIC ("fracture","ICD10","S826","Lower leg fracture","",""),
# MAGIC ("fracture","ICD10","S827","Lower leg fracture","",""),
# MAGIC ("fracture","ICD10","S828","Lower leg fracture","",""),
# MAGIC ("fracture","ICD10","S829","Lower leg fracture","",""),
# MAGIC ("fracture","ICD10","S920","Foot fracture","",""),
# MAGIC ("fracture","ICD10","S921","Foot fracture","",""),
# MAGIC ("fracture","ICD10","S922","Foot fracture","",""),
# MAGIC ("fracture","ICD10","S923","Foot fracture","",""),
# MAGIC ("fracture","ICD10","S927","Foot fracture","",""),
# MAGIC ("fracture","ICD10","S929","Foot fracture","",""),
# MAGIC ("fracture","ICD10","T12","Fracture of lower limb","",""),
# MAGIC ("fracture","ICD10","T025","Fractures involving multiple regions of both lower limbs","",""),
# MAGIC ("fracture","ICD10","T023","Fractures involving multiple regions of both lower limbs","","")
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Arterial dissection

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view ccu035_01_artery_dissect as
# MAGIC select *
# MAGIC from values
# MAGIC 
# MAGIC ("artery_dissect","ICD10","I71.0","Dissection of aorta [any part]","",""),
# MAGIC ("artery_dissect","ICD10","I72.0","Aneurysm and dissection of carotid artery","",""),
# MAGIC ("artery_dissect","ICD10","I72.1","Aneurysm and dissection of artery of upper extremity","",""),
# MAGIC ("artery_dissect","ICD10","I72.6","Dissection of vertebral artery","","")
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Life threatening arrhythmias

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view ccu035_01_life_arrhythmias as
# MAGIC select *
# MAGIC from values
# MAGIC 
# MAGIC ("life_arrhythmia","ICD10","I46.0","Cardiac arrest with successful resuscitation","",""),
# MAGIC ("life_arrhythmia","ICD10","I46.1 ","Sudden cardiac death, so described","",""),
# MAGIC ("life_arrhythmia","ICD10","I46.9","Cardiac arrest, unspecified","",""),
# MAGIC ("life_arrhythmia","ICD10","I47.0","Re-entry ventricular arrhythmia","",""),
# MAGIC ("life_arrhythmia","ICD10","I47.2","Ventricular tachycardia","",""),
# MAGIC ("life_arrhythmia","ICD10","I49.0","Ventricular fibrillation and flutter","","")
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cardiomyopathy

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view ccu035_01_cardiomyopathy as
# MAGIC select *
# MAGIC from values
# MAGIC 
# MAGIC ("cardiomyopathy","ICD10","I42.0","Dilated cardiomyopathy","1","20210127"),
# MAGIC ("cardiomyopathy","ICD10","I42.3","Endomyocardial (eosinophilic) disease","1","20210127"),
# MAGIC ("cardiomyopathy","ICD10","I42.5","Other restrictive cardiomyopathy","1","20210127"),
# MAGIC ("cardiomyopathy","ICD10","I42.7","Cardiomyopathy due to drugs and other external agents","1","20210127"),
# MAGIC ("cardiomyopathy","ICD10","I42.8","Other cardiomyopathies","1","20210127"),
# MAGIC ("cardiomyopathy","ICD10","I42.9","Cardiomyopathy, unspecified","1","20210127"),
# MAGIC ("cardiomyopathy","ICD10","I43","Cardiomyopathy in diseases classified elsewhere","1","20210127"),
# MAGIC ("cardiomyopathy","ICD10","I25.5 ","Ischaemic cardiomyopathy","1","20210127"),
# MAGIC ("cardiomyopathy","ICD10","O90.3 ","Cardiomyopathy in the puerperium","1","20210127")
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate)
# MAGIC union all
# MAGIC select *
# MAGIC from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
# MAGIC where name like 'cardiomyopathy'
# MAGIC and terminology like 'SNOMED'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Heart failure

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view ccu035_01_HF as
# MAGIC select *
# MAGIC from values
# MAGIC 
# MAGIC ("HF","ICD10","I50","Heart failure","",""),
# MAGIC ("HF","ICD10","I11.0","Hypertensive heart disease with (congestive) heart failure","",""),
# MAGIC ("HF","ICD10","I13.0","Hypertensive heart and renal disease with (congestive) heart failure","",""),
# MAGIC ("HF","ICD10","I13.2","Hypertensive heart and renal disease with both (congestive) heart failure and renal failure","",""),
# MAGIC ("HF","SNOMED","10335000","Chronic right-sided heart failure (disorder)","1","20210127"),
# MAGIC ("HF","SNOMED","10633002","Acute congestive heart failure","1","20210127"),
# MAGIC ("HF","SNOMED","42343007","Congestive heart failure","1","20210127"),
# MAGIC ("HF","SNOMED","43736008","Rheumatic left ventricular failure","1","20210127"),
# MAGIC ("HF","SNOMED","48447003","Chronic heart failure (disorder)","1","20210127"),
# MAGIC ("HF","SNOMED","56675007","Acute heart failure","1","20210127"),
# MAGIC ("HF","SNOMED","71892000","Cardiac asthma","1","20210127"),
# MAGIC ("HF","SNOMED","79955004","Chronic cor pulmonale","1","20210127"),
# MAGIC ("HF","SNOMED","83105008","Malignant hypertensive heart disease with congestive heart failure","1","20210127"),
# MAGIC ("HF","SNOMED","84114007","Heart failure","1","20210127"),
# MAGIC ("HF","SNOMED","85232009","Left heart failure","1","20210127"),
# MAGIC ("HF","SNOMED","87837008","Chronic pulmonary heart disease","1","20210127"),
# MAGIC ("HF","SNOMED","88805009","Chronic congestive heart failure","1","20210127"),
# MAGIC ("HF","SNOMED","92506005","Biventricular congestive heart failure","1","20210127"),
# MAGIC ("HF","SNOMED","128404006","Right heart failure","1","20210127"),
# MAGIC ("HF","SNOMED","134401001","Left ventricular systolic dysfunction","1","20210127"),
# MAGIC ("HF","SNOMED","134440006","Referral to heart failure clinic","1","20210127"),
# MAGIC ("HF","SNOMED","194767001","Benign hypertensive heart disease with congestive cardiac failure","1","20210127"),
# MAGIC ("HF","SNOMED","194779001","Hypertensive heart and renal disease with (congestive) heart failure","1","20210127"),
# MAGIC ("HF","SNOMED","194781004","Hypertensive heart and renal disease with both (congestive) heart failure and renal failure","1","20210127"),
# MAGIC ("HF","SNOMED","195111005","Decompensated cardiac failure","1","20210127"),
# MAGIC ("HF","SNOMED","195112003","Compensated cardiac failure","1","20210127"),
# MAGIC ("HF","SNOMED","195114002","Acute left ventricular failure","1","20210127"),
# MAGIC ("HF","SNOMED","206586007","Congenital cardiac failure","1","20210127"),
# MAGIC ("HF","SNOMED","233924009","Heart failure as a complication of care (disorder)","1","20210127"),
# MAGIC ("HF","SNOMED","275514001","Impaired left ventricular function","1","20210127"),
# MAGIC ("HF","SNOMED","314206003","Refractory heart failure (disorder)","1","20210127"),
# MAGIC ("HF","SNOMED","367363000","Right ventricular failure","1","20210127"),
# MAGIC ("HF","SNOMED","407596008","Echocardiogram shows left ventricular systolic dysfunction (finding)","1","20210127"),
# MAGIC ("HF","SNOMED","420300004","New York Heart Association Classification - Class I (finding)","1","20210127"),
# MAGIC ("HF","SNOMED","420913000","New York Heart Association Classification - Class III (finding)","1","20210127"),
# MAGIC ("HF","SNOMED","421704003","New York Heart Association Classification - Class II (finding)","1","20210127"),
# MAGIC ("HF","SNOMED","422293003","New York Heart Association Classification - Class IV (finding)","1","20210127"),
# MAGIC ("HF","SNOMED","426263006","Congestive heart failure due to left ventricular systolic dysfunction (disorder)","1","20210127"),
# MAGIC ("HF","SNOMED","426611007","Congestive heart failure due to valvular disease (disorder)","1","20210127"),
# MAGIC ("HF","SNOMED","430396006","Chronic systolic dysfunction of left ventricle (disorder)","1","20210127"),
# MAGIC ("HF","SNOMED","446221000","Heart failure with normal ejection fraction (disorder)","1","20210127"),
# MAGIC ("HF","SNOMED","698592004","Asymptomatic left ventricular systolic dysfunction (disorder)","1","20210127"),
# MAGIC ("HF","SNOMED","703272007","Heart failure with reduced ejection fraction (disorder)","1","20210127"),
# MAGIC ("HF","SNOMED","717491000000102","Excepted from heart failure quality indicators - informed dissent (finding)","1","20210127"),
# MAGIC ("HF","SNOMED","760361000000100","Fast track heart failure referral for transthoracic two dimensional echocardiogram","1","20210127")
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Angina & Unstable angina

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view ccu035_01_angina as
# MAGIC select *
# MAGIC from values
# MAGIC 
# MAGIC ("angina","ICD10","I20","Angina","",""),
# MAGIC ("angina","SNOMED","10971000087107","Myocardial ischemia during surgery (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","15960061000119102","Unstable angina co-occurrent and due to coronary arteriosclerosis (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","15960581000119102","Angina co-occurrent and due to arteriosclerosis of autologous vein coronary artery bypass graft (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","15960661000119107","Unstable angina co-occurrent and due to arteriosclerosis of coronary artery bypass graft (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","19057007","Status anginosus (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","194828000","Angina (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","21470009","Syncope anginosa (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","233821000","New onset angina (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","25106000","Impending infarction (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","314116003","Post infarct angina (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","371806006","Progressive angina (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","371809004","Recurrent angina status post coronary stent placement (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","371812001","Recurrent angina status post directional coronary atherectomy (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","41334000","Angina, class II (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","413439005","Acute ischemic heart disease (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","413444003","Acute myocardial ischemia (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","413838009","Chronic ischemic heart disease (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","429559004","Typical angina (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","4557003","Preinfarction syndrome (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","59021001","Angina decubitus (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","61490001","Angina, class I (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","712866001","Resting ischemia co-occurrent and due to ischemic heart disease (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","791000119109","Angina associated with type II diabetes mellitus (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","85284003","Angina, class III (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","89323001","Angina, class IV (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","15960141000119102","Angina co-occurrent and due to coronary arteriosclerosis (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","15960381000119109","Angina co-occurrent and due to arteriosclerosis of coronary artery bypass graft (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","194823009","Acute coronary insufficiency (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","225566008","Ischemic chest pain (finding)","1","20210127"),
# MAGIC ("angina","SNOMED","233819005","Stable angina (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","233823002","Silent myocardial ischemia (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","300995000","Exercise-induced angina (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","315025001","Refractory angina (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","35928006","Nocturnal angina (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","371807002","Atypical angina (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","371808007","Recurrent angina status post percutaneous transluminal coronary angioplasty (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","371810009","Recurrent angina status post coronary artery bypass graft (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","371811008","Recurrent angina status post rotational atherectomy (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","394659003","Acute coronary syndrome (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","413844008","Chronic myocardial ischemia (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","414545008","Ischemic heart disease (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","414795007","Myocardial ischemia (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","46109009","Subendocardial ischemia (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","697976003","Microvascular ischemia of myocardium (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","703214003","Silent coronary vasospastic disease (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","713405002","Subacute ischemic heart disease (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","87343002","Prinzmetal angina (disorder)","1","20210127"),
# MAGIC ("unstable_angina","ICD10","I20.1","Unstable angina","",""),
# MAGIC ("unstable_angina","SNOMED","15960061000119102","Unstable angina co-occurrent and due to coronary arteriosclerosis (disorder)","1","20210127"),
# MAGIC ("unstable_angina","SNOMED","15960661000119107","Unstable angina co-occurrent and due to arteriosclerosis of coronary artery bypass graft (disorder)","1","20210127"),
# MAGIC ("unstable_angina","SNOMED","19057007","Status anginosus (disorder)","1","20210127"),
# MAGIC ("unstable_angina","SNOMED","25106000","Impending infarction (disorder)","1","20210127"),
# MAGIC ("unstable_angina","SNOMED","4557003","Preinfarction syndrome (disorder)","1","20210127"),
# MAGIC ("unstable_angina","SNOMED","712866001","Resting ischemia co-occurrent and due to ischemic heart disease (disorder)","1","20210127"),
# MAGIC ("unstable_angina","SNOMED","315025001","Refractory angina (disorder)","1","20210127"),
# MAGIC ("unstable_angina","SNOMED","35928006","Nocturnal angina (disorder)","1","20210127"),
# MAGIC ("unstable_angina","SNOMED","371807002","Atypical angina (disorder)","1","20210127"),
# MAGIC ("unstable_angina","SNOMED","394659003","Acute coronary syndrome (disorder)","1","20210127")
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pericarditis & Myocarditis

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view ccu035_01_pericarditis_myocarditis as
# MAGIC select *
# MAGIC from values
# MAGIC 
# MAGIC ("pericarditis","ICD10","I30","Acute pericarditis","",""),
# MAGIC ("myocarditis","ICD10","I51.4","Myocarditis, unspecified","",""),
# MAGIC ("myocarditis","ICD10","I40","Acute myocarditis","",""),
# MAGIC ("myocarditis","ICD10","I41","Myocarditis in diseases classified elsewhere","","")
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stroke_TIA

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view ccu035_01_stroke_TIA as
# MAGIC select *
# MAGIC from values
# MAGIC 
# MAGIC ("stroke_TIA","ICD10","G45","Transient cerebral ischaemic attacks and related syndromes","",""),
# MAGIC ("stroke_TIA","SNOMED","195206000","Intermittent cerebral ischaemia","1","20210127"),
# MAGIC ("stroke_TIA","SNOMED","230716006","Anterior circulation transient ischaemic attack","1","20210127"),
# MAGIC ("stroke_TIA","SNOMED","266257000","TIA","1","20210127"),
# MAGIC ("stroke_TIA","SNOMED","230717002","Vertebrobasilar territory transient ischemic attack (disorder)","1","20210127"),
# MAGIC ("stroke_TIA","SNOMED","230716006","Anterior circulation transient ischaemic attack","1","20210127"),
# MAGIC ("stroke_TIA","SNOMED","230717002","Vertebrobasilar territory transient ischaemic attack","1","20210127")
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ### COVID codes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SNOMED COVID-19 codes
# MAGIC -- Create Temporary View with of COVID-19 codes and their grouping - to be used whilst waiting for them to be uploaded onto the TR
# MAGIC -- Covid-1 Status groups:
# MAGIC -- - Lab confirmed incidence
# MAGIC -- - Lab confirmed historic
# MAGIC -- - Clinically confirmed
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu035_01_snomed_codes_covid19 AS
# MAGIC SELECT *
# MAGIC FROM VALUES
# MAGIC ("1008541000000105","Coronavirus ribonucleic acid detection assay (observable entity)","0","1","Lab confirmed incidence"),
# MAGIC ("1029481000000103","Coronavirus nucleic acid detection assay (observable entity)","0","1","Lab confirmed incidence"),
# MAGIC ("120814005","Coronavirus antibody (substance)","0","1","Lab confirmed historic"),
# MAGIC ("121973000","Measurement of coronavirus antibody (procedure)","0","1","Lab confirmed historic"),
# MAGIC ("1240381000000105","Severe acute respiratory syndrome coronavirus 2 (organism)","0","1","Clinically confirmed"),
# MAGIC ("1240391000000107","Antigen of severe acute respiratory syndrome coronavirus 2 (substance)","0","1","Lab confirmed incidence"),
# MAGIC ("1240401000000105","Antibody to severe acute respiratory syndrome coronavirus 2 (substance)","0","1","Lab confirmed historic"),
# MAGIC ("1240411000000107","Ribonucleic acid of severe acute respiratory syndrome coronavirus 2 (substance)","0","1","Lab confirmed incidence"),
# MAGIC ("1240421000000101","Serotype severe acute respiratory syndrome coronavirus 2 (qualifier value)","0","1","Lab confirmed historic"),
# MAGIC ("1240511000000106","Detection of severe acute respiratory syndrome coronavirus 2 using polymerase chain reaction technique (procedure)","0","1","Lab confirmed incidence"),
# MAGIC ("1240521000000100","Otitis media caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1240531000000103","Myocarditis caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1240541000000107","Infection of upper respiratory tract caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1240551000000105","Pneumonia caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1240561000000108","Encephalopathy caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1240571000000101","Gastroenteritis caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1240581000000104","Severe acute respiratory syndrome coronavirus 2 ribonucleic acid detected (finding)","0","1","Lab confirmed incidence"),
# MAGIC ("1240741000000103","Severe acute respiratory syndrome coronavirus 2 serology (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1240751000000100","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1300631000000101","Coronavirus disease 19 severity score (observable entity)","0","1","Clinically confirmed"),
# MAGIC ("1300671000000104","Coronavirus disease 19 severity scale (assessment scale)","0","1","Clinically confirmed"),
# MAGIC ("1300681000000102","Assessment using coronavirus disease 19 severity scale (procedure)","0","1","Clinically confirmed"),
# MAGIC ("1300721000000109","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 confirmed by laboratory test (situation)","0","1","Lab confirmed historic"),
# MAGIC ("1300731000000106","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 confirmed using clinical diagnostic criteria (situation)","0","1","Clinically confirmed"),
# MAGIC ("1321181000000108","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 record extraction simple reference set (foundation metadata concept)","0","1","Clinically confirmed"),
# MAGIC ("1321191000000105","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 procedures simple reference set (foundation metadata concept)","0","1","Clinically confirmed"),
# MAGIC ("1321201000000107","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 health issues simple reference set (foundation metadata concept)","0","1","Clinically confirmed"),
# MAGIC ("1321211000000109","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 presenting complaints simple reference set (foundation metadata concept)","0","1","Clinically confirmed"),
# MAGIC ("1321241000000105","Cardiomyopathy caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1321301000000101","Severe acute respiratory syndrome coronavirus 2 ribonucleic acid qualitative existence in specimen (observable entity)","0","1","Lab confirmed incidence"),
# MAGIC ("1321311000000104","Severe acute respiratory syndrome coronavirus 2 immunoglobulin M qualitative existence in specimen (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1321321000000105","Severe acute respiratory syndrome coronavirus 2 immunoglobulin G qualitative existence in specimen (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1321331000000107","Arbitrary concentration of severe acute respiratory syndrome coronavirus 2 total immunoglobulin in serum (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1321341000000103","Arbitrary concentration of severe acute respiratory syndrome coronavirus 2 immunoglobulin G in serum (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1321351000000100","Arbitrary concentration of severe acute respiratory syndrome coronavirus 2 immunoglobulin M in serum (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1321541000000108","Severe acute respiratory syndrome coronavirus 2 immunoglobulin G detected (finding)","0","1","Lab confirmed historic"),
# MAGIC ("1321551000000106","Severe acute respiratory syndrome coronavirus 2 immunoglobulin M detected (finding)","0","1","Lab confirmed historic"),
# MAGIC ("1321761000000103","Severe acute respiratory syndrome coronavirus 2 immunoglobulin A detected (finding)","0","1","Lab confirmed historic"),
# MAGIC ("1321801000000108","Arbitrary concentration of severe acute respiratory syndrome coronavirus 2 immunoglobulin A in serum (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1321811000000105","Severe acute respiratory syndrome coronavirus 2 immunoglobulin A qualitative existence in specimen (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1322781000000102","Severe acute respiratory syndrome coronavirus 2 antigen detection result positive (finding)","0","1","Lab confirmed incidence"),
# MAGIC ("1322871000000109","Severe acute respiratory syndrome coronavirus 2 antibody detection result positive (finding)","0","1","Lab confirmed historic"),
# MAGIC ("186747009","Coronavirus infection (disorder)","0","1","Clinically confirmed")
# MAGIC 
# MAGIC AS tab(clinical_code, description, sensitive_status, include_binary, covid_status);

# COMMAND ----------

# MAGIC %md
# MAGIC ### COVID vaccination codes

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu035_01_codelist_covid_vaccine_products AS
# MAGIC SELECT *
# MAGIC FROM VALUES
# MAGIC ("covid vaccination", "SNOMED","39114911000001105","COVID19_vaccine_AstraZeneca", "1","n/a"),
# MAGIC ("covid vaccination", "SNOMED","39115011000001105","COVID19_vaccine_AstraZeneca", "1","n/a"),
# MAGIC ("covid vaccination", "SNOMED","39115111000001106","COVID19_vaccine_AstraZeneca", "1","n/a"),
# MAGIC ("covid vaccination", "SNOMED","39115711000001107","COVID19_vaccine_Pfizer", "1","n/a"),
# MAGIC ("covid vaccination", "SNOMED","39115611000001103","COVID19_vaccine_Pfizer", "1","n/a"),
# MAGIC ("covid vaccination", "SNOMED","39326911000001101","COVID19_vaccine_Moderna", "1","n/a"), 
# MAGIC ("covid vaccination", "SNOMED","39375411000001104","COVID19_vaccine_Moderna", "1","n/a"),
# MAGIC ("covid vaccination", "SNOMED","1324681000000101","COVID19_vaccine_dose1", "1","n/a"),
# MAGIC ("covid vaccination", "SNOMED","1324691000000104","COVID19_vaccine_dose2", "1","n/a"),
# MAGIC ("covid vaccination", "SNOMED","61396006","COVID19_vaccine_site_left_thigh", "1","n/a"),
# MAGIC ("covid vaccination", "SNOMED","368209003","COVID19_vaccine_site_right_upper_arm", "1","n/a"),
# MAGIC ("covid vaccination", "SNOMED","368208006","COVID19_vaccine_site_left_upper_arm", "1","n/a"),
# MAGIC ("covid vaccination", "SNOMED","723980000","COVID19_vaccine_site_right_buttock", "1","n/a"),
# MAGIC ("covid vaccination", "SNOMED","723979003","COVID19_vaccine_site_left_buttock", "1","n/a"),
# MAGIC ("covid vaccination", "SNOMED","11207009","COVID19_vaccine_site_right_thigh", "1","n/a"),
# MAGIC ("covid vaccination", "SNOMED","413294000","COVID19_vaccine_care_setting_community_health_services", "1","n/a"),
# MAGIC ("covid vaccination", "SNOMED","310065000","COVID19_vaccine_care_setting_open_access_service", "1","n/a"),
# MAGIC ("covid vaccination", "SNOMED","788007007","COVID19_vaccine_care_setting_general_practice_service", "1","n/a")
# MAGIC 
# MAGIC AS tab(name, terminology, code, term, code_type, RecordDate);

# COMMAND ----------

# MAGIC %md
# MAGIC ### COPD codes (JQK)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SNOMED COPD
# MAGIC -- Create Temporary View with of COPD codes and their grouping - to be used whilst waiting for them to be uploaded onto the TR
# MAGIC -- COPD diagnosis:
# MAGIC -- - Clinically confirmed
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu035_01_snomed_codes_copd AS
# MAGIC SELECT *
# MAGIC FROM VALUES
# MAGIC ("copd","SNOMED","106001000119101","chronic obstructive lung disease co-occurrent with acute bronchitis (disorder)","1","20220215"),
# MAGIC ("copd","SNOMED","135836000","end stage chronic obstructive airways disease (disorder)","1","20220215"),
# MAGIC ("copd","SNOMED","13645005","chronic obstructive lung disease (disorder)","1","20220215"),
# MAGIC ("copd","SNOMED","16003001","giant bullous emphysema (disorder)","1","20220215"),
# MAGIC ("copd","SNOMED","16846004","obstructive emphysema (disorder)","1","20220215"),
# MAGIC ("copd","SNOMED","185086009","chronic obstructive bronchitis (disorder)","1","20220215"),
# MAGIC ("copd","SNOMED","195951007","acute exacerbation of chronic obstructive airways disease (disorder)","1","20220215"),
# MAGIC ("copd","SNOMED","195958001","segmental bullous emphysema (disorder)","1","20220215"),
# MAGIC ("copd","SNOMED","195959009","zonal bullous emphysema (disorder)","1","20220215"),
# MAGIC ("copd","SNOMED","196001008","chronic obstructive pulmonary disease with acute lower respiratory infection (disorder)","1","20220215"),
# MAGIC ("copd","SNOMED","233674008","pulmonary emphysema in alpha-1 primary immunodeficiency deficiency (disorder)","1","20220215"),
# MAGIC ("copd","SNOMED","266355005","bullous emphysema with collapse (disorder)","1","20220215"),
# MAGIC ("copd","SNOMED","285381006","acute infective exacerbation of chronic obstructive airways disease (disorder)","1","20220215"),
# MAGIC ("copd","SNOMED","293241000119100","acute exacerbation of chronic obstructive bronchitis (disorder)","1","20220215"),
# MAGIC ("copd","SNOMED","293991000000106","very severe chronic obstructive pulmonary disease (disorder)","1","20220215"),
# MAGIC ("copd","SNOMED","313296004","mild chronic obstructive pulmonary disease (disorder)","1","20220215"),
# MAGIC ("copd","SNOMED","313297008","moderate chronic obstructive pulmonary disease (disorder)","1","20220215"),
# MAGIC ("copd","SNOMED","313299006","severe chronic obstructive pulmonary disease (disorder)","1","20220215"),
# MAGIC ("copd","SNOMED","31898008","paraseptal emphysema (disorder)","1","20220215"),
# MAGIC ("copd","SNOMED","45145000","unilateral emphysema (situation)","1","20220215"),
# MAGIC ("copd","SNOMED","4981000","panacinar emphysema (disorder)","1","20220215"),
# MAGIC ("copd","SNOMED","66110007","chronic diffuse emphysema caused by inhalation of chemical fumes and/or vapors (disorder)","1","20220215"),
# MAGIC ("copd","SNOMED","68328006","centriacinar emphysema (disorder)","1","20220215"),
# MAGIC ("copd","SNOMED","847091000000104","acute non-infective exacerbation of chronic obstructive pulmonary disease (disorder)","1","20220215"),
# MAGIC ("copd","SNOMED","87433001","pulmonary emphysema (disorder)","1","20220215"),
# MAGIC ("copd","SNOMED","408501008","emergency hospital admission for chronic obstructive pulmonary disease (procedure)","1","20220215"),
# MAGIC ("copd","SNOMED","723245007","number of chronic obstructive pulmonary disease exacerbations in past year (observable entity)","1","20220215"),
# MAGIC ("copd","SNOMED","1066231000000103","chronic obstructive pulmonary disease monitoring email invitation (procedure)","1","20220215"),
# MAGIC ("copd","SNOMED","1066281000000104","chronic obstructive pulmonary disease monitoring short message service text message invitation (procedure)","1","20220215"),
# MAGIC ("copd","SNOMED","1066821000000101","chronic obstructive pulmonary disease monitoring short message service text message first invitation (procedure)","1","20220215"),
# MAGIC ("copd","SNOMED","1066841000000108","chronic obstructive pulmonary disease monitoring short message service text message second invitation (procedure)","1","20220215"),
# MAGIC ("copd","SNOMED","1066851000000106","chronic obstructive pulmonary disease monitoring short message service text message third invitation (procedure)","1","20220215"),
# MAGIC ("copd","SNOMED","1110861000000102","quality and outcomes framework chronic obstructive pulmonary disease quality indicator-related care invitation (procedure)","1","20220215"),
# MAGIC ("copd","SNOMED","143371000000104","quality and outcomes framework chronic obstructive pulmonary disease quality indicator-related care invitation using preferred method of communication (procedure)","1","20220215"),
# MAGIC ("copd","SNOMED","711431000000109","chronic obstructive pulmonary disease monitoring invitation (procedure)","1","20220215"),
# MAGIC ("copd","SNOMED","716241000000106","chronic obstructive pulmonary disease monitoring first letter (procedure)","1","20220215"),
# MAGIC ("copd","SNOMED","716281000000103","chronic obstructive pulmonary disease monitoring verbal invitation (procedure)","1","20220215"),
# MAGIC ("copd","SNOMED","716901000000101","chronic obstructive pulmonary disease monitoring telephone invitation (procedure)","1","20220215"),
# MAGIC ("copd","SNOMED","717021000000106","chronic obstructive pulmonary disease monitoring second letter (procedure)","1","20220215"),
# MAGIC ("copd","SNOMED","717521000000104","chronic obstructive pulmonary disease monitoring third letter (procedure)","1","20220215"),
# MAGIC ("copd","SNOMED","716291000000101","excepted from chronic obstructive pulmonary disease quality indicators - informed dissent (finding)","1","20220215"),
# MAGIC ("copd","SNOMED","716311000000100","excepted from chronic obstructive pulmonary disease quality indicators - patient unsuitable (finding)","1","20220215"),
# MAGIC ("copd","SNOMED","1108721000000102","excepted from chronic obstructive pulmonary disease quality indicators - service unavailable (finding)","1","20220215"),
# MAGIC ("copd","SNOMED","941201000000103","chronic obstructive pulmonary disease resolved (finding)","1","20220215"),
# MAGIC ("copd","SNOMED","394703002","chronic obstructive pulmonary disease annual review (regime/therapy)","1","20220215"),
# MAGIC ("copd","SNOMED","760601000000107","chronic obstructive pulmonary disease 3 monthly review (regime/therapy)","1","20220215"),
# MAGIC ("copd","SNOMED","760621000000103","chronic obstructive pulmonary disease 6 monthly review (regime/therapy)","1","20220215"),
# MAGIC ("copd","SNOMED","371611000000107","chronic obstructive pulmonary disease patient unsuitable for pulmonary rehabilitation (finding)","1","20220215"),
# MAGIC ("copd","SNOMED","836477007","chronic emphysema caused by vapor (disorder)","1","20220215"),
# MAGIC ("copd","SNOMED","1010333003","emphysema of left lung (disorder)","1","20220215"),
# MAGIC ("copd","SNOMED","1010334009","emphysema of right lung (disorder)","1","20220215")
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronchiectasis codes (JQK)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SNOMED Bronchiectasis
# MAGIC -- Create Temporary View with of bronchiectasis codes and their grouping - to be used whilst waiting for them to be uploaded onto the TR
# MAGIC -- Bronchiectasis diagnosis:
# MAGIC -- - Clinically confirmed
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu035_01_snomed_codes_bronchiectasis AS
# MAGIC SELECT *
# MAGIC FROM VALUES
# MAGIC ("bronchiectasis","SNOMED","12295008","bronchiectasis (disorder)","1","20220215"),
# MAGIC ("bronchiectasis","SNOMED","195984007","recurrent bronchiectasis (disorder)","1","20220215"),
# MAGIC ("bronchiectasis","SNOMED","195985008","post-infective bronchiectasis (disorder)","1","20220215"),
# MAGIC ("bronchiectasis","SNOMED","23022004","tuberculous bronchiectasis (disorder)","1","20220215"),
# MAGIC ("bronchiectasis","SNOMED","233627004","congenital cystic bronchiectasis (disorder)","1","20220215"),
# MAGIC ("bronchiectasis","SNOMED","233628009","acquired bronchiectasis (disorder)","1","20220215"),
# MAGIC ("bronchiectasis","SNOMED","233629001","idiopathic bronchiectasis (disorder)","1","20220215"),
# MAGIC ("bronchiectasis","SNOMED","233630006","obstructive bronchiectasis (disorder)","1","20220215"),
# MAGIC ("bronchiectasis","SNOMED","233631005","toxin-induced bronchiectasis (disorder)","1","20220215"),
# MAGIC ("bronchiectasis","SNOMED","714203003","acute bronchitis co-occurrent with bronchiectasis (disorder)","1","20220215"),
# MAGIC ("bronchiectasis","SNOMED","12310001","childhood bronchiectasis (disorder)","1","20220215"),
# MAGIC ("bronchiectasis","SNOMED","13217005","fusiform bronchiectasis (disorder)","1","20220215"),
# MAGIC ("bronchiectasis","SNOMED","19325002","traction bronchiectasis (disorder)","1","20220215"),
# MAGIC ("bronchiectasis","SNOMED","445378003","acute exacerbation of bronchiectasis (disorder)","1","20220215"),
# MAGIC ("bronchiectasis","SNOMED","51068008","adult bronchiectasis (disorder)","1","20220215"),
# MAGIC ("bronchiectasis","SNOMED","52500008","saccular bronchiectasis (disorder)","1","20220215"),
# MAGIC ("bronchiectasis","SNOMED","83457000","cylindrical bronchiectasis (disorder)","1","20220215"),
# MAGIC ("bronchiectasis","SNOMED","77593006","congenital bronchiectasis (disorder)","1","20220215"),
# MAGIC ("bronchiectasis","SNOMED","879963005","exacerbation of bronchiectasis caused by infection (disorder)","1","20220215")
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cystic fibrosis codes (JQK)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SNOMED Cystic fibrosis
# MAGIC -- Create Temporary View with of CF codes and their grouping - to be used whilst waiting for them to be uploaded onto the TR
# MAGIC -- CF diagnosis:
# MAGIC -- - Clinically confirmed
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu035_01_snomed_codes_cf AS
# MAGIC SELECT *
# MAGIC FROM VALUES
# MAGIC ("CF","SNOMED","235978006","cystic fibrosis of pancreas (disorder)","1","20220215"),
# MAGIC ("CF","SNOMED","427089005","diabetes mellitus due to cystic fibrosis (disorder)","1","20220215"),
# MAGIC ("CF","SNOMED","698940002","arthropathy associated with cystic fibrosis (disorder)","1","20220215"),
# MAGIC ("CF","SNOMED","707418001","male infertility due to cystic fibrosis (disorder)","1","20220215"),
# MAGIC ("CF","SNOMED","707419009","osteoporosis due to cystic fibrosis (disorder)","1","20220215"),
# MAGIC ("CF","SNOMED","707420003","portal hypertension due to cystic fibrosis (disorder)","1","20220215"),
# MAGIC ("CF","SNOMED","707450006","pancreatic insufficiency due to cystic fibrosis of pancreas (disorder)","1","20220215"),
# MAGIC ("CF","SNOMED","707536003","digestive system manifestation co-occurrent and due to cystic fibrosis (disorder)","1","20220215"),
# MAGIC ("CF","SNOMED","707542004","otorhinolaryngological manifestation co-occurrent and due to cystic fibrosis (disorder)","1","20220215"),
# MAGIC ("CF","SNOMED","707577004","female infertility due to cystic fibrosis (disorder)","1","20220215"),
# MAGIC ("CF","SNOMED","707578009","perforation of intestine due to cystic fibrosis with meconium ileus (disorder)","1","20220215"),
# MAGIC ("CF","SNOMED","707734002","elevated liver enzymes level due to cystic fibrosis (disorder)","1","20220215"),
# MAGIC ("CF","SNOMED","707766007","exocrine pancreatic manifestation co-occurrent and due to cystic fibrosis (disorder)","1","20220215"),
# MAGIC ("CF","SNOMED","720401009","cystic fibrosis with gastritis and megaloblastic anemia syndrome (disorder)","1","20220215"),
# MAGIC ("CF","SNOMED","721197001","polyneuropathy due to classical cystic fibrosis (disorder)","1","20220215"),
# MAGIC ("CF","SNOMED","762269004","classical cystic fibrosis (disorder)","1","20220215"),
# MAGIC ("CF","SNOMED","762270003","atypical cystic fibrosis (disorder)","1","20220215"),
# MAGIC ("CF","SNOMED","190905008","cystic fibrosis (disorder)","1","20220215"),
# MAGIC ("CF","SNOMED","190909002","cystic fibrosis with intestinal manifestations (disorder)","1","20220215"),
# MAGIC ("CF","SNOMED","426705001","diabetes mellitus co-occurrent and due to cystic fibrosis (disorder)","1","20220215"),
# MAGIC ("CF","SNOMED","427022004","liver disease due to cystic fibrosis (disorder)","1","20220215"),
# MAGIC ("CF","SNOMED","526071000000104","arthropathy in cystic fibrosis (disorder)","1","20220215"),
# MAGIC ("CF","SNOMED","526091000000100","cystic fibrosis with distal intestinal obstruction syndrome (disorder)","1","20220215"),
# MAGIC ("CF","SNOMED","716088000","follicular hamartoma with alopecia and cystic fibrosis syndrome (disorder)","1","20220215"),
# MAGIC ("CF","SNOMED","776981000000103","cirrhosis associated with cystic fibrosis (disorder)","1","20220215"),
# MAGIC ("CF","SNOMED","81423003","cystic fibrosis without meconium ileus (disorder)","1","20220215"),
# MAGIC ("CF","SNOMED","859041000000103","exacerbation of cystic fibrosis (disorder)","1","20220215"),
# MAGIC ("CF","SNOMED","86092005","cystic fibrosis with meconium ileus (disorder)","1","20220215"),
# MAGIC ("CF","SNOMED","86555001","cystic fibrosis of the lung (disorder)","1","20220215"),
# MAGIC ("CF","SNOMED","762271004","subclinical cystic fibrosis (disorder)","1","20220215"),
# MAGIC ("CF","SNOMED","707734002","elevated liver enzymes level due to cystic fibrosis (finding)","1","20220215"),
# MAGIC ("CF","SNOMED","817966005","distal intestinal obstruction syndrome due to cystic fibrosis (disorder)","1","20220215"),
# MAGIC ("CF","SNOMED","778131000000103","cystic fibrosis related cirrhosis","1","20220215"),
# MAGIC ("CF","SNOMED","16434671000119101","cystic fibrosis transmembrane conductance regulator related metabolic syndrome (finding)","1","20220215"),
# MAGIC ("CF","SNOMED","1010616001","cirrhosis of liver due to classical cystic fibrosis (disorder)","1","20220215")
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Asthma codes JKQ

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SNOMED asthma
# MAGIC -- Create Temporary View with of asthma codes and their grouping - to be used whilst waiting for them to be uploaded onto the TR
# MAGIC -- asthma diagnosis:
# MAGIC -- - Clinically confirmed
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu035_01_snomed_codes_asthma AS
# MAGIC SELECT *
# MAGIC FROM VALUES
# MAGIC ("asthma","SNOMED","1064811000000103","moderate acute exacerbation of asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","1064821000000109","life threatening acute exacerbation of asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","10674711000119105","acute severe exacerbation of asthma co-occurrent with allergic rhinitis (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","10675391000119101","severe controlled persistent asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","10675431000119106","severe persistent allergic asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","10675471000119109","acute severe exacerbation of severe persistent allergic asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","10675551000119104","acute severe exacerbation of severe persistent asthma co-occurrent with allergic rhinitis (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","10675591000119109","severe persistent allergic asthma controlled (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","10675631000119109","severe persistent asthma controlled co-occurrent with allergic rhinitis (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","10675671000119107","severe persistent allergic asthma uncontrolled (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","10675711000119106","severe persistent asthma uncontrolled co-occurrent with allergic rhinitis (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","10675751000119107","severe uncontrolled persistent asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","10675871000119106","mild persistent allergic asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","10675911000119109","acute severe exacerbation of mild persistent allergic asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","10675991000119100","acute severe exacerbation of mild persistent allergic asthma co-occurrent with allergic rhinitis (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","10676031000119106","mild persistent allergic asthma controlled (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","10676071000119109","mild persistent asthma controlled (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","10676111000119102","mild persistent asthma controlled co-occurrent with allergic rhinitis (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","10676151000119101","mild persistent allergic asthma uncontrolled (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","10676191000119106","mild persistent asthma uncontrolled (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","10676231000119102","mild persistent asthma uncontrolled co-occurrent with allergic rhinitis (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","10676351000119103","moderate persistent asthma controlled (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","10676391000119108","moderate persistent allergic asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","10676431000119103","acute severe exacerbation of moderate persistent allergic asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","10676511000119109","acute severe exacerbation of moderate persistent asthma co-occurrent with allergic rhinitis (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","10676551000119105","moderate persistent allergic asthma controlled (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","10676591000119100","moderate persistent controlled asthma co-occurrent with allergic rhinitis (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","10676631000119100","moderate persistent allergic asthma uncontrolled (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","10676671000119102","moderate persistent asthma uncontrolled co-occurrent with allergic rhinitis (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","10676711000119103","moderate persistent asthma uncontrolled (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","10692721000119102","chronic obstructive asthma co-occurrent with acute exacerbation of asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","10742121000119104","asthma in mother complicating childbirth (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","1086701000000102","life threatening acute exacerbation of allergic asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","1086711000000100","life threatening acute exacerbation of intrinsic asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","1103911000000103","severe asthma with fungal sensitisation (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","11641008","millers' asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","12428000","intrinsic asthma without status asthmaticus (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","124991000119109","severe persistent asthma co-occurrent with allergic rhinitis (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","125001000119103","moderate persistent asthma co-occurrent with allergic rhinitis (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","125011000119100","mild persistent asthma co-occurrent with allergic rhinitis (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","125021000119107","intermittent asthma co-occurrent with allergic rhinitis (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","135171000119106","acute exacerbation of moderate persistent asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","135181000119109","acute exacerbation of mild persistent asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","16584951000119101","oral steroid-dependent asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","1741000119102","intermittent asthma uncontrolled (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","1751000119100","acute exacerbation of chronic obstructive airways disease with asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","18041002","printers' asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","195949008","chronic asthmatic bronchitis (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","195967001","asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","195977004","mixed asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","225057002","brittle asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","233679003","late onset asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","233683003","hay fever with asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","233688007","sulfite-induced asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","233691007","asthmatic pulmonary eosinophilia (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","2360001000004109","steroid dependent asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","266361008","non-allergic asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","281239006","exacerbation of asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","30352005","allergic-infective asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","304527002","acute asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","312453004","asthma - currently active (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","31387002","exercise-induced asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","34015007","bakers' asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","370218001","mild asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","370219009","moderate asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","370220003","occasional asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","370221004","severe asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","389145006","allergic asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","395022009","asthma night-time symptoms (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","401000119107","asthma with irreversible airway obstruction (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","401193004","asthma confirmed (situation)","1","20220215"),
# MAGIC ("asthma","SNOMED","404804003","platinum asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","404806001","cheese-makers' asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","404808000","isocyanate induced asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","405944004","asthmatic bronchitis (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","407674008","aspirin-induced asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","409663006","cough variant asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","41553006","detergent asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","418395004","tea-makers' asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","423889005","non-immunoglobulin e mediated allergic asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","424199006","substance induced asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","424643009","immunoglobulin e-mediated allergic asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","425969006","exacerbation of intermittent asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","426656000","severe persistent asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","426979002","mild persistent asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","427295004","moderate persistent asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","427603009","intermittent asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","427679007","mild intermittent asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","442025000","acute exacerbation of chronic asthmatic bronchitis (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","445427006","seasonal asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","55570000","asthma without status asthmaticus (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","56968009","asthma caused by wood dust (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","57607007","occupational asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","63088003","allergic asthma without status asthmaticus (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","641000119106","intermittent asthma well controlled (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","703953004","allergic asthma caused by dermatophagoides pteronyssinus (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","703954005","allergic asthma caused by dermatophagoides farinae (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","707444001","uncomplicated asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","707445000","exacerbation of mild persistent asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","707446004","exacerbation of moderate persistent asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","707447008","exacerbation of severe persistent asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","707511009","uncomplicated mild persistent asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","707512002","uncomplicated moderate persistent asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","707513007","uncomplicated severe persistent asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","707979007","acute severe exacerbation of severe persistent asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","707980005","acute severe exacerbation of moderate persistent asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","707981009","acute severe exacerbation of mild persistent asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","708038006","acute exacerbation of asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","708090002","acute severe exacerbation of asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","708093000","acute exacerbation of immunoglobulin e-mediated allergic asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","708094006","acute exacerbation of intrinsic asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","708095007","acute severe exacerbation of immunoglobin e-mediated allergic asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","708096008","acute severe exacerbation of intrinsic asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","72301000119103","asthma in pregnancy (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","733858005","acute severe refractory exacerbation of asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","734904007","life threatening acute exacerbation of asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","734905008","moderate acute exacerbation of asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","735587000","acute severe exacerbation of asthma co-occurrent and due to allergic asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","735588005","uncomplicated allergic asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","735589002","uncomplicated non-allergic asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","762521001","exacerbation of allergic asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","782513000","acute severe exacerbation of allergic asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","782520007","exacerbation of allergic asthma due to infection (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","786836003","near fatal asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","866881000000101","chronic asthma with fixed airflow obstruction (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","92807009","chemical-induced asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","93432008","drug-induced asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","99031000119107","acute exacerbation of asthma co-occurrent with allergic rhinitis (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","183478001","emergency hospital admission for asthma (procedure)","1","20220215"),
# MAGIC ("asthma","SNOMED","708358003","emergency asthma admission since last encounter (situation)","1","20220215"),
# MAGIC ("asthma","SNOMED","443117005","asthma control test score (observable entity)","1","20220215"),
# MAGIC ("asthma","SNOMED","763077003","asthma control questionnaire score (observable entity)","1","20220215"),
# MAGIC ("asthma","SNOMED","905301000000103","childhood asthma control test score (observable entity)","1","20220215"),
# MAGIC ("asthma","SNOMED","366874008","number of asthma exacerbations in past year (observable entity)","1","20220215"),
# MAGIC ("asthma","SNOMED","1110841000000103","quality and outcomes framework asthma quality indicator-related care invitation (procedure)","1","20220215"),
# MAGIC ("asthma","SNOMED","185731000","asthma monitoring call first letter (procedure)","1","20220215"),
# MAGIC ("asthma","SNOMED","185732007","asthma monitoring call second letter (procedure)","1","20220215"),
# MAGIC ("asthma","SNOMED","185734008","asthma monitoring call third letter (procedure)","1","20220215"),
# MAGIC ("asthma","SNOMED","185735009","asthma monitoring call verbal invite (procedure)","1","20220215"),
# MAGIC ("asthma","SNOMED","185736005","asthma monitoring call telephone invite (procedure)","1","20220215"),
# MAGIC ("asthma","SNOMED","92161000000104","quality and outcomes framework asthma quality indicator-related care invitation using preferred method of communication (procedure)","1","20220215"),
# MAGIC ("asthma","SNOMED","928451000000107","asthma monitoring invitation short message service text message (procedure)","1","20220215"),
# MAGIC ("asthma","SNOMED","928511000000107","asthma monitoring invitation email (procedure)","1","20220215"),
# MAGIC ("asthma","SNOMED","959401000000101","asthma monitoring short message service text message first invitation (procedure)","1","20220215"),
# MAGIC ("asthma","SNOMED","959421000000105","asthma monitoring short message service text message second invitation (procedure)","1","20220215"),
# MAGIC ("asthma","SNOMED","959441000000103","asthma monitoring short message service text message third invitation (procedure)","1","20220215"),
# MAGIC ("asthma","SNOMED","716491000000100","excepted from asthma quality indicators - informed dissent (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","717291000000103","excepted from asthma quality indicators - patient unsuitable (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","1108701000000106","excepted from asthma quality indicators - service unavailable (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","162660004","asthma resolved (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","170631002","asthma disturbing sleep (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","170632009","asthma causing night waking (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","170633004","asthma disturbs sleep weekly (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","170634005","asthma disturbs sleep frequently (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","170635006","asthma not disturbing sleep (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","170636007","asthma never disturbs sleep (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","170637003","asthma limiting activities (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","170638008","asthma not limiting activities (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","170655007","asthma restricts exercise (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","170656008","asthma sometimes restricts exercise (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","170657004","asthma severely restricts exercise (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","170658009","asthma never restricts exercise (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","270442000","asthma monitoring check done (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","370202007","asthma causes daytime symptoms 1 to 2 times per month (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","370203002","asthma causes daytime symptoms 1 to 2 times per week (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","370204008","asthma causes daytime symptoms most days (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","370205009","asthma causes night symptoms 1 to 2 times per month (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","370206005","asthma limits walking on the flat (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","370207001","asthma limits walking up hills or stairs (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","370208006","asthma never causes daytime symptoms (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","373899003","asthma daytime symptoms (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","390872009","change in asthma management plan (regime/therapy)","1","20220215"),
# MAGIC ("asthma","SNOMED","390877003","step up change in asthma management plan (regime/therapy)","1","20220215"),
# MAGIC ("asthma","SNOMED","390878008","step down change in asthma management plan (regime/therapy)","1","20220215"),
# MAGIC ("asthma","SNOMED","390921001","absent from work or school due to asthma (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","394700004","asthma annual review (regime/therapy)","1","20220215"),
# MAGIC ("asthma","SNOMED","394701000","asthma follow-up (regime/therapy)","1","20220215"),
# MAGIC ("asthma","SNOMED","394720003","asthma medication review (procedure)","1","20220215"),
# MAGIC ("asthma","SNOMED","401182001","asthma monitoring by nurse (regime/therapy)","1","20220215"),
# MAGIC ("asthma","SNOMED","401183006","asthma monitoring by doctor (regime/therapy)","1","20220215"),
# MAGIC ("asthma","SNOMED","473391009","asthma never causes night symptoms (situation)","1","20220215"),
# MAGIC ("asthma","SNOMED","754061000000100","asthma review using royal college of physicians three questions (regime/therapy)","1","20220215"),
# MAGIC ("asthma","SNOMED","771901000000100","asthma causes night time symptoms 1 to 2 times per week (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","771941000000102","asthma causes symptoms most nights (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","771981000000105","asthma limits activities 1 to 2 times per month (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","772011000000107","asthma limits activities 1 to 2 times per week (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","772051000000106","asthma limits activities most days (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","811151000000105","number of days absent from school due to asthma in past 6 months (observable entity)","1","20220215"),
# MAGIC ("asthma","SNOMED","527171000000103","patient has a written asthma personal action plan (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","810901000000102","asthma self-management plan review (procedure)","1","20220215"),
# MAGIC ("asthma","SNOMED","811921000000103","asthma self-management plan agreed (finding)","1","20220215"),
# MAGIC ("asthma","SNOMED","10674991000119104","intermittent allergic asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","829976001","thunderstorm asthma (disorder)","1","20220215"),
# MAGIC ("asthma","SNOMED","782559003","asthma never causes night symptoms (finding)","1","20220215")
# MAGIC 
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate);

# COMMAND ----------

# MAGIC %md
# MAGIC ### ILD codes JKQ

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SNOMED ILD
# MAGIC -- Create Temporary View with of ILD codes and their grouping - to be used whilst waiting for them to be uploaded onto the TR
# MAGIC -- ILD diagnosis:
# MAGIC -- - Clinically confirmed
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu035_01_snomed_codes_ild AS
# MAGIC SELECT *
# MAGIC FROM VALUES
# MAGIC ("ILD","SNOMED","233748006","simple pneumoconiosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","233749003","complicated pneumoconiosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","233750003","erionite pneumoconiosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","233751004","metal pneumoconiosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","233754007","cerium pneumoconiosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","233755008","nickel pneumoconiosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","233756009","thorium pneumoconiosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","233757000","zirconium pneumoconiosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","233758005","mica pneumoconiosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","233759002","mixed mineral dust pneumoconiosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","233761006","subacute silicosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","233762004","chronic silicosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","233764003","wollastonite pneumoconiosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","26511004","pneumoconiosis caused by sisal fiber (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","29422001","coal workers' pneumoconiosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","32139003","mixed dust pneumoconiosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","398640008","rheumatoid pneumoconiosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","40122008","pneumoconiosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","40218008","carbon electrode makers' pneumoconiosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","426853005","pneumoconiosis caused by silicate (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","58691003","antimony pneumoconiosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","73144008","pneumoconiosis caused by talc (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","805002","pneumoconiosis caused by silica (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","87909002","hard metal pneumoconiosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","32544004","chronic obliterative bronchiolitis caused by inhalation of chemical fumes and/or vapors (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","37711000","cadmium pneumonitis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","47515009","simple silicosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","8549006","desquamative interstitial pneumonia (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","236302005","acute interstitial pneumonia (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","35037009","primary atypical interstitial pneumonia (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","385479009","follicular bronchiolitis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","4120002","bronchiolitis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","430476004","diffuse panbronchiolitis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","59903001","acute obliterating bronchiolitis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","64667001","interstitial pneumonia (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","704345008","chronic interstitial pneumonia (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","10713006","diffuse interstitial rheumatoid disease of lung (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","129452008","nonspecific interstitial pneumonia (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","196051003","drug-induced interstitial lung disorder (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","196133001","lung disease with systemic sclerosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","233692000","cryptogenic pulmonary eosinophilia (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","233770009","stage 4 pulmonary sarcoidosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","24369008","pulmonary sarcoidosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","30042003","confluent fibrosis of lung (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","34371004","subacute obliterative bronchiolitis caused by inhalation of chemical fumes and/or vapors (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","3514002","peribronchial fibrosis of lung (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","405570007","pulmonary fibrosis due to and following radiotherapy (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","427046006","drug-induced pneumonitis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","54867000","rheumatoid fibrosing alveolitis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","56841008","massive fibrosis of lung (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","700249006","idiopathic interstitial pneumonia (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","707434003","pulmonary fibrosis due to hermansky-pudlak syndrome (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","719218000","cryptogenic organizing pneumonia (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","733453005","congenital nephrotic syndrome, interstitial lung disease, epidermolysis bullosa syndrome (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","737181009","interstitial lung disease due to systemic disease (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","737182002","interstitial lung disease due to granulomatous disease (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","737183007","interstitial lung disease due to metabolic disease (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","737184001","interstitial lung disease co-occurrent and due to systemic vasculitis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","155621007","rheumatoid lung (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","196132006","rheumatoid lung (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","201794001","rheumatoid lung (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","201813004","(rheumatoid lung) or (caplan's syndrome) or (fibrosing alveolitis associated with rheumatoid arthritis) (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","239794002","fibrosing alveolitis associated with rheumatoid arthritis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","319841000119107","rheumatoid lung disease with rheumatoid arthritis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","398726004","rheumatoid lung disease (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","129451001","respiratory bronchiolitis associated interstitial lung disease (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","129458007","bronchiolitis obliterans organizing pneumonia (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","14700006","bauxite fibrosis of lung (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","17385007","graphite fibrosis of lung (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","17996008","pneumoconiosis caused by inorganic dust (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","192658007","giant cell interstitial pneumonitis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","196027008","toxic bronchiolitis obliterans (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","196053000","chronic drug-induced interstitial lung disorders (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","196125002","diffuse interstitial pulmonary fibrosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","22607003","asbestosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","233673002","drug-induced bronchiolitis obliterans (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","233703007","interstitial lung disease (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","233713004","seasonal cryptogenic organizing pneumonia with biochemical cholestasis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","233723008","bronchiolitis obliterans with usual interstitial pneumonitis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","233724002","toxic diffuse interstitial pulmonary fibrosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","233725001","drug-induced diffuse interstitial pulmonary fibrosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","233760007","acute silicosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","277844007","pulmonary lymphangioleiomyomatosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","36599006","chronic fibrosis of lung (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","40100001","obliterative bronchiolitis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","40527005","idiopathic pulmonary hemosiderosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","40640008","massive fibrosis of lung co-occurrent and due to silicosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","427123006","interstitial lung disease due to collagen vascular disease (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","44274007","lymphoid interstitial pneumonia (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","47938003","chronic obliterative bronchiolitis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","51615001","fibrosis of lung (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","62371005","pulmonary siderosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","711379004","interstitial lung disease due to connective tissue disease (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","71193007","fibrosis of lung caused by radiation (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","72270005","collagenous pneumoconiosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","196136009","lung disease co-occurrent with polymyositis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","836478002","subacute obliterative bronchiolitis caused by chemical fumes (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","836479005","subacute obliterative bronchiolitis caused by vapor (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","840350008","chronic obliterative bronchiolitis caused by chemical fumes (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","840351007","chronic obliterative bronchiolitis caused by vapor (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","866103007","interstitial lung disease due to juvenile polymyositis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","239297008","lymphomatoid granulomatosis of lung (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","870573008","interstitial pneumonia with autoimmune features (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","1017196003","interstitial pulmonary fibrosis due to inhalation of substance (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","1017197007","interstitial pulmonary fibrosis due to inhalation of drug (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","700252003","subacute idiopathic pulmonary fibrosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","708537005","acute idiopathic pulmonary fibrosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","426437004","familial idiopathic pulmonary fibrosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","700250006","idiopathic pulmonary fibrosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","700251005","chronic idiopathic pulmonary fibrosis (disorder)","1","20220215"),
# MAGIC ("ILD","SNOMED","789574002","acute exacerbation of idiopathic pulmonary fibrosis (disorder)","1","20220215")
# MAGIC 
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Appending codelists

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW inf_codelist AS
# MAGIC SELECT
# MAGIC   codelist AS name,
# MAGIC   system AS terminology,
# MAGIC   code,
# MAGIC   term,
# MAGIC   "" AS code_type,
# MAGIC   "" AS RecordDate
# MAGIC FROM
# MAGIC   global_temp.ccu035_01_drug_codelists
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   smoking_status AS name,
# MAGIC   'SNOMED' AS terminology,
# MAGIC   conceptID AS code,
# MAGIC   description AS term,
# MAGIC   "" AS code_type,
# MAGIC   "" AS RecordDate
# MAGIC FROM
# MAGIC   global_temp.ccu035_01_smokingstatus_SNOMED
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC 'pregnancy_and_birth' AS name,
# MAGIC 'SNOMED' AS terminology,
# MAGIC code,
# MAGIC term,
# MAGIC "" AS code_type,
# MAGIC  "" AS RecordDate
# MAGIC FROM
# MAGIC global_temp.ccu035_01_pregnancy_birth_sno
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC 'prostate_cancer' AS name,
# MAGIC 'SNOMED' AS terminology,
# MAGIC code,
# MAGIC term,
# MAGIC "" AS code_type,
# MAGIC  "" AS RecordDate
# MAGIC FROM
# MAGIC global_temp.ccu035_01_prostate_cancer_sno
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC 'depression' AS name,
# MAGIC terminology,
# MAGIC code,
# MAGIC term,
# MAGIC code_type,
# MAGIC RecordDate
# MAGIC FROM
# MAGIC global_temp.ccu035_01_depression
# MAGIC union all
# MAGIC select *
# MAGIC from global_temp.ccu035_01_BMI_obesity
# MAGIC union all
# MAGIC select *
# MAGIC from global_temp.ccu035_01_hypertension
# MAGIC union all
# MAGIC select *
# MAGIC from global_temp.ccu035_01_diabetes
# MAGIC union all 
# MAGIC select *
# MAGIC from global_temp.ccu035_01_cancer
# MAGIC union all 
# MAGIC select *
# MAGIC from global_temp.ccu035_01_liver
# MAGIC union all 
# MAGIC select *
# MAGIC from global_temp.ccu035_01_dementia
# MAGIC union all 
# MAGIC select *
# MAGIC from global_temp.ccu035_01_ckd
# MAGIC union all
# MAGIC select 
# MAGIC new_name as name,
# MAGIC terminology,
# MAGIC code,
# MAGIC term,
# MAGIC code_type,
# MAGIC RecordDate
# MAGIC from global_temp.ccu035_01_ami
# MAGIC union all
# MAGIC select *
# MAGIC from global_temp.ccu035_01_vt
# MAGIC union all
# MAGIC select * 
# MAGIC from global_temp.ccu035_01_DVT_ICVT
# MAGIC union all
# MAGIC select * 
# MAGIC from global_temp.ccu035_01_PE
# MAGIC union all
# MAGIC select * 
# MAGIC from global_temp.ccu035_01_stroke_IS
# MAGIC union all
# MAGIC select 
# MAGIC new_name as name,
# MAGIC terminology,
# MAGIC code,
# MAGIC term,
# MAGIC code_type,
# MAGIC RecordDate
# MAGIC from global_temp.ccu035_01_stroke_NOS
# MAGIC union all
# MAGIC select 
# MAGIC new_name as name,
# MAGIC terminology,
# MAGIC code,
# MAGIC term,
# MAGIC code_type,
# MAGIC RecordDate
# MAGIC from global_temp.ccu035_01_stroke_SAH
# MAGIC union all
# MAGIC select 
# MAGIC new_name as name,
# MAGIC terminology,
# MAGIC code,
# MAGIC term,
# MAGIC code_type,
# MAGIC RecordDate
# MAGIC from global_temp.ccu035_01_stroke_HS
# MAGIC union all
# MAGIC select * 
# MAGIC from global_temp.ccu035_01_thrombophilia
# MAGIC union all
# MAGIC select * 
# MAGIC from global_temp.ccu035_01_TCP
# MAGIC union all
# MAGIC select * 
# MAGIC from global_temp.ccu035_01_retinal_infarction
# MAGIC union all
# MAGIC select * 
# MAGIC from global_temp.ccu035_01_arterial_embolism
# MAGIC union all
# MAGIC select * 
# MAGIC from global_temp.ccu035_01_DIC
# MAGIC union all
# MAGIC select * 
# MAGIC from global_temp.ccu035_01_mesenteric_thrombus
# MAGIC union all
# MAGIC select * 
# MAGIC from global_temp.ccu035_01_spinal_stroke
# MAGIC union all
# MAGIC select * 
# MAGIC from global_temp.ccu035_01_fracture
# MAGIC union all
# MAGIC select * 
# MAGIC from global_temp.ccu035_01_artery_dissect
# MAGIC union all
# MAGIC select * 
# MAGIC from global_temp.ccu035_01_life_arrhythmias
# MAGIC union all
# MAGIC select * 
# MAGIC from global_temp.ccu035_01_cardiomyopathy
# MAGIC union all
# MAGIC select * 
# MAGIC from global_temp.ccu035_01_HF
# MAGIC union all
# MAGIC select * 
# MAGIC from global_temp.ccu035_01_angina
# MAGIC union all
# MAGIC select * 
# MAGIC from global_temp.ccu035_01_pericarditis_myocarditis
# MAGIC union all
# MAGIC select * 
# MAGIC from global_temp.ccu035_01_stroke_TIA
# MAGIC union all
# MAGIC select *
# MAGIC from global_temp.ccu035_01_snomed_codes_copd
# MAGIC union all 
# MAGIC select *
# MAGIC from global_temp.ccu035_01_snomed_codes_bronchiectasis
# MAGIC union all 
# MAGIC select *
# MAGIC from global_temp.ccu035_01_snomed_codes_cf
# MAGIC union all 
# MAGIC select *
# MAGIC from global_temp.ccu035_01_snomed_codes_asthma
# MAGIC union all 
# MAGIC select *
# MAGIC from global_temp.ccu035_01_snomed_codes_ild
# MAGIC union all 
# MAGIC select *
# MAGIC from global_temp.ccu035_01_codelist_covid_vaccine_products
# MAGIC union all
# MAGIC 
# MAGIC SELECT
# MAGIC   'GDPPR_confirmed_COVID' AS name,
# MAGIC   'SNOMED' AS terminology,
# MAGIC   clinical_code AS code,
# MAGIC   description AS term,
# MAGIC   "" AS code_type,
# MAGIC   "" AS RecordDate
# MAGIC from global_temp.ccu035_01_snomed_codes_covid19

# COMMAND ----------

drop_table(project_prefix + codelist_final_name, if_exists = True)

# COMMAND ----------

create_table(project_prefix + codelist_final_name , select_sql_script=f"SELECT * FROM global_temp.inf_codelist") 

# COMMAND ----------

# MAGIC %sql
# MAGIC select name
# MAGIC from dars_nic_391419_j3w9t_collab.ccu035_01_codelist
# MAGIC where name like '%DVT%'
# MAGIC -- codelist used for the project containing ICD10/SNOMED codes 

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from dars_nic_391419_j3w9t_collab.ccu035_01_codelist
# MAGIC where name like '%DVT%'

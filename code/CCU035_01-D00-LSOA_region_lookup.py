# Databricks notebook source
# MAGIC 
# MAGIC %md # CCU035_01-D00-LSOA_region_lookup
# MAGIC  
# MAGIC **Description** Creates a LSOA to region lookup table. This code was originally from `dars_nic_391419_j3w9t_collab/DATA_CURATION/wrang901_geography` by Sam Hollings.
# MAGIC 
# MAGIC **Author(s)** Hannah Whittaker
# MAGIC 
# MAGIC **Project(s)** CCU035_01
# MAGIC 
# MAGIC **Reviewer(s)** 
# MAGIC  
# MAGIC **Date last updated** 2022-02-09
# MAGIC  
# MAGIC **Date last reviewed** 
# MAGIC  
# MAGIC **Date last run** 2022-02-09
# MAGIC  
# MAGIC **Data input** `dss_corporate.ons_chd_geo_listings`
# MAGIC 
# MAGIC **Data output** 
# MAGIC 
# MAGIC **Software and versions** SQL, Python
# MAGIC  
# MAGIC **Packages and versions** Not applicable

# COMMAND ----------

# MAGIC %run "/Workspaces/dars_nic_391419_j3w9t_collab/CCU035_01/CCU035_01-functions/wrang000_functions"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or replace global temporary view  ccu035_01_lsoa_region_lookup AS
# MAGIC with 
# MAGIC curren_chd_geo_listings as (SELECT * FROM dss_corporate.ons_chd_geo_listings WHERE IS_CURRENT = 1),
# MAGIC lsoa_auth as (
# MAGIC   SELECT e01.geography_code as lsoa_code, e01.geography_name lsoa_name, 
# MAGIC   e02.geography_code as msoa_code, e02.geography_name as msoa_name, 
# MAGIC   e0789.geography_code as authority_code, e0789.geography_name as authority_name,
# MAGIC   e0789.parent_geography_code as authority_parent_geography
# MAGIC   FROM curren_chd_geo_listings e01
# MAGIC   LEFT JOIN curren_chd_geo_listings e02 on e02.geography_code = e01.parent_geography_code
# MAGIC   LEFT JOIN curren_chd_geo_listings e0789 on e0789.geography_code = e02.parent_geography_code
# MAGIC   WHERE e01.geography_code like 'E01%' and e02.geography_code like 'E02%'
# MAGIC ),
# MAGIC auth_county as (
# MAGIC   SELECT lsoa_code, lsoa_name,
# MAGIC          msoa_code, msoa_name,
# MAGIC          authority_code, authority_name,
# MAGIC          e10.geography_code as county_code, e10.geography_name as county_name,
# MAGIC          e10.parent_geography_code as parent_geography
# MAGIC   FROM 
# MAGIC   lsoa_auth
# MAGIC     LEFT JOIN dss_corporate.ons_chd_geo_listings e10 on e10.geography_code = lsoa_auth.authority_parent_geography
# MAGIC   
# MAGIC   WHERE LEFT(authority_parent_geography,3) = 'E10'
# MAGIC ),
# MAGIC auth_met_county as (
# MAGIC   SELECT lsoa_code, lsoa_name,
# MAGIC            msoa_code, msoa_name,
# MAGIC            authority_code, authority_name,
# MAGIC            NULL as county_code, NULL as county_name,           
# MAGIC            lsoa_auth.authority_parent_geography as region_code
# MAGIC   FROM lsoa_auth
# MAGIC   WHERE LEFT(authority_parent_geography,3) = 'E12'
# MAGIC ),
# MAGIC lsoa_region_code as (
# MAGIC   SELECT lsoa_code, lsoa_name,
# MAGIC            msoa_code, msoa_name,
# MAGIC            authority_code, authority_name,
# MAGIC            county_code, county_name, auth_county.parent_geography as region_code
# MAGIC   FROM auth_county
# MAGIC   UNION ALL
# MAGIC   SELECT lsoa_code, lsoa_name,
# MAGIC            msoa_code, msoa_name,
# MAGIC            authority_code, authority_name,
# MAGIC            county_code, county_name, region_code 
# MAGIC   FROM auth_met_county
# MAGIC ),
# MAGIC lsoa_region as (
# MAGIC   SELECT lsoa_code, lsoa_name,
# MAGIC            msoa_code, msoa_name,
# MAGIC            authority_code, authority_name,
# MAGIC            county_code, county_name, region_code, e12.geography_name as region_name FROM lsoa_region_code
# MAGIC   LEFT JOIN dss_corporate.ons_chd_geo_listings e12 on lsoa_region_code.region_code = e12.geography_code
# MAGIC )
# MAGIC SELECT * FROM lsoa_region

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists dars_nic_391419_j3w9t_collab.ccu035_01_lsoa_region_lookup

# COMMAND ----------

# MAGIC %sql
# MAGIC create table dars_nic_391419_j3w9t_collab.CCU035_01_lsoa_region_lookup as
# MAGIC SELECT * FROM global_temp.ccu035_01_lsoa_region_lookup

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE dars_nic_391419_j3w9t_collab.ccu035_01_lsoa_region_lookup OWNER TO dars_nic_391419_j3w9t_collab

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT (lsoa_code) FROM  dars_nic_391419_j3w9t_collab.ccu035_01_lsoa_region_lookup 

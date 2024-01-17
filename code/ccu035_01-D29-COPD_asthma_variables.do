*Before creating these variables, need to create a list of patient identifiers with the variables asthma and copd
*Once all variables are created we can then merge these into the main analysis file that Costas has. 
*Also load in the final table created in Databricks and save to AWS so easier for future. 

*list of patient idenifiers ******************************************
*note: this will be a different file for you Costas, so relace with the name of your final analysis file
cd "D:\Users\h.whittaker\Documents"

use "ccu035_01_final_analysis_cohort_2.dta", clear
keep  nhs_number_deid copd asthma covid19_confirmed_date
save "D:\Users\h.whittaker\Documents\patid_index_date.dta", replace
***********************************************************************

*load in final covariates table*****************************************
clear all
odbc load, exec ("SELECT * FROM dars_nic_391419_j3w9t_collab.ccu035_02_out_covariates_flags") dsn("databricks")
rename *, lower
save "D:\Users\h.whittaker\Documents\single_covariates.dta", replace

clear all
odbc load, exec ("SELECT * FROM dars_nic_391419_j3w9t_collab.ccu035_02_out_covariates_washout") dsn("databricks")
rename *, lower
save "D:\Users\h.whittaker\Documents\multi_covariates.dta", replace
***********************************************************************

***********************************
*Poor asthma control ********
**********************************
*Exposure: >=2 OCS or >=1 A&E hosp/admission (already combined in variable) or >=6 SABA in last 12 months

*OCS
use "D:\Users\h.whittaker\Documents\multi_covariates.dta", clear
keep if name=="ocs_asthma_date"
keep nhs_number_deid date
merge m:1 nhs_number_deid using "D:\Users\h.whittaker\Documents\patid_index_date.dta"
keep if _m==3 
drop _m
sort nhs_number_deid date
by nhs_number_deid: gen tot_ocs=_N
keep if tot_ocs>=2
keep nhs_number_deid
duplicates drop
gen ocs2=1
save "D:\Users\h.whittaker\Documents\ocs_for_asthma_control.dta", replace

*A&E 
use "D:\Users\h.whittaker\Documents\multi_covariates.dta", clear
keep if name=="asthma_hosp_date"
merge m:1 nhs_number_deid using "D:\Users\h.whittaker\Documents\patid_index_date.dta"
keep if _m==3
drop _m
keep nhs_number_deid date
keep nhs_number_deid
duplicates drop
gen astma_hosp=1
save "D:\Users\h.whittaker\Documents\hosp_for_asthma_control.dta", replace

*SABA
use "D:\Users\h.whittaker\Documents\single_covariates.dta", clear
keep nhs_number_deid tot_saba 
merge m:1 nhs_number_deid using "D:\Users\h.whittaker\Documents\patid_index_date.dta"
keep if _m==3
drop _m
destring tot_saba, replace
keep if tot_saba!=.
duplicates drop
keep nhs_number_deid
gen saba2=1
save "D:\Users\h.whittaker\Documents\saba_for_asthma_control.dta", replace

*Merge all these three files together to create final asthma control variable
use "D:\Users\h.whittaker\Documents\saba_for_asthma_control.dta", clear
merge 1:1 nhs_number_deid using "D:\Users\h.whittaker\Documents\ocs_for_asthma_control.dta"
drop _m
merge 1:1 nhs_number_deid using "D:\Users\h.whittaker\Documents\hosp_for_asthma_control.dta"
drop _m
duplicates drop 
gen poor_asthma_control=1
save "D:\Users\h.whittaker\Documents\asthma_control_final.dta", replace


*************************************
*Asthma exacerbations ************
************************************
*either treated with ocs, recorded in GP, or A&E/admission event

*OCS
use "D:\Users\h.whittaker\Documents\multi_covariates.dta", clear
keep if name=="ocs_asthma_date"
keep nhs_number_deid date
merge m:1 nhs_number_deid using "D:\Users\h.whittaker\Documents\patid_index_date.dta"
keep if _m==3 
drop _m
keep nhs_number_deid date
duplicates drop
gen mild=1
save "D:\Users\h.whittaker\Documents\ocs_asthma_exac.dta"

*GP-no codes here other than 1...so didnt use
*HES 
use "D:\Users\h.whittaker\Documents\multi_covariates.dta", clear
keep if name=="asthma_hosp_date"
keep nhs_number_deid date
merge m:1 nhs_number_deid using "D:\Users\h.whittaker\Documents\patid_index_date.dta"
keep if _m==3 
drop _m
keep nhs_number_deid date
duplicates drop
gen severe=1
save "D:\Users\h.whittaker\Documents\hosp_asthma_exac.dta"

merge 1:1 nhs_number_deid date using "D:\Users\h.whittaker\Documents\ocs_asthma_exac.dta"
drop _m

*just make sure events arent within 2 weeks of eachother
sort nhs_number_deid date
by nhs_number_deid: gen diff=date-date[_n-1]
tab diff
keep if diff==. | diff>14 & diff!=.

*make a mild and severe variable
keep nhs_number_deid mild severe
sort nhs_number_deid
by nhs_number_deid: gen tot_mild=sum(mild) if mild!=.
by nhs_number_deid: gen tot_severe=sum(severe) if severe!=.
by nhs_number_deid: egen tot_mild2=max(tot_mild)
by nhs_number_deid: egen tot_severe2=max(tot_severe)
recode tot_mild2 .=0
recode tot_severe2 .=0
drop mild severe tot_mild tot_severe
duplicates drop

*now combine mild and severe with the number of exacerbations
gen tot_asthma_exac=1 if tot_mild2==1 & tot_severe2==0
replace tot_asthma_exac=2 if tot_mild2>=2 & tot_severe2==0
replace tot_asthma_exac=3 if tot_severe2==1
replace tot_asthma_exac=4 if tot_severe2>=2
label define  lab1 1"1 mild, 0 severe" 2"2+ mild, 0 severe"  3"1 severe, any mild" 4"2+ severe, any mild"
label values tot_asthma_exac lab1

*keep final variable and save
keep nhs_number_deid tot_asthma_exac 
duplicates drop
save "D:\Users\h.whittaker\Documents\asthma_exacerbations_final.dta", replace

********************************************************************
*Asthma annual review
********************************************************************
use "D:\Users\h.whittaker\Documents\single_covariates.dta", clear
keep if asthma_review==1
*merge with patient cohort
merge m:1 nhs_number_deid using "D:\Users\h.whittaker\Documents\patid_index_date.dta"
keep if _m==3 
drop _m
keep nhs_number_deid asthma_review
duplicates drop
save "D:\Users\h.whittaker\Documents\asthma_review_final.dta", replace

************************************************************************
*Asthma medication check
*****************************************
use "D:\Users\h.whittaker\Documents\single_covariates.dta", clear
keep if asthma_med_check==1
*merge with patient cohort
merge m:1 nhs_number_deid using "D:\Users\h.whittaker\Documents\patid_index_date.dta"
keep if _m==3 
drop _m
keep nhs_number_deid asthma_med_check
duplicates drop
save "D:\Users\h.whittaker\Documents\asthma_med_check_final.dta", replace


****************************
*ICS for COPD=exposure 
*********************************
use "D:\Users\h.whittaker\Documents\single_covariates.dta", clear
keep if ics_use==1
*merge with patient cohort
merge m:1 nhs_number_deid using "D:\Users\h.whittaker\Documents\patid_index_date.dta"
keep if _m==3 
drop _m
keep nhs_number_deid ics_use
duplicates drop
save "D:\Users\h.whittaker\Documents\ics_use_final.dta", replace

*******************
*SABA/SAMA for COPD
******************************
use "D:\Users\h.whittaker\Documents\single_covariates.dta", clear
keep if sama_saba==1
*merge with patient cohort
merge m:1 nhs_number_deid using "D:\Users\h.whittaker\Documents\patid_index_date.dta"
keep if _m==3 
drop _m
keep nhs_number_deid sama_saba 
duplicates drop
save "D:\Users\h.whittaker\Documents\saba_sama_use_final.dta", replace

********************
*height for COPD
********************
use "D:\Users\h.whittaker\Documents\single_covariates.dta", clear
destring height, replace
keep if height!=.
*merge with patient cohort
merge m:1 nhs_number_deid using "D:\Users\h.whittaker\Documents\patid_index_date.dta"
keep if _m==3 
drop _m
keep nhs_number_deid height 
duplicates drop

*m
gen height2=height if height>1 & height<=3
*cm
replace height2=height/100 if height>100 & height<=300
*inches/feet
replace height2=height/3.281 if height>4 & height<=10

drop height 
rename height2 height
keep if height!=.
save "D:\Users\h.whittaker\Documents\height_final.dta", replace

***********************
*MRC for COPD
***************************
use "D:\Users\h.whittaker\Documents\single_covariates.dta", clear
destring mrc_value, replace
keep if mrc_value!=.
*merge with patient cohort
merge m:1 nhs_number_deid using "D:\Users\h.whittaker\Documents\patid_index_date.dta"
keep if _m==3 
drop _m

keep nhs_number_deid mrc_value
duplicates drop
save "D:\Users\h.whittaker\Documents\mrc_final.dta", replace

**************************
*GORD for COPD
***************************
use "D:\Users\h.whittaker\Documents\single_covariates.dta", clear
keep if gord==1
*merge with patient cohort
merge m:1 nhs_number_deid using "D:\Users\h.whittaker\Documents\patid_index_date.dta"
keep if _m==3 
drop _m

keep nhs_number_deid gord 
duplicates drop
save "D:\Users\h.whittaker\Documents\gord_final.dta", replace

**************************
*AECOPD
*************************
*GP
use "D:\Users\h.whittaker\Documents\multi_covariates.dta", clear
keep if name=="gp_aecopd_date"
keep nhs_number_deid date
*merge with patient cohort
merge m:1 nhs_number_deid using "D:\Users\h.whittaker\Documents\patid_index_date.dta"
keep if _m==3 
drop _m
keep nhs_number_deid date
gen gp=1
save "D:\Users\h.whittaker\Documents\gp_aecopd.dta"

*HES
use "D:\Users\h.whittaker\Documents\multi_covariates.dta", clear
keep if name=="aecopd_hes_date"
keep nhs_number_deid date
*merge with patient cohort
merge m:1 nhs_number_deid using "D:\Users\h.whittaker\Documents\patid_index_date.dta"
keep if _m==3 
drop _m
keep nhs_number_deid date
gen hes=1
save "D:\Users\h.whittaker\Documents\hes_aecopd.dta"

append using "D:\Users\h.whittaker\Documents\gp_aecopd.dta"
sort nhs_number_deid date

*just make sure events arent within 2 weeks of eachother
by nhs_number_deid: gen diff=date-date[_n-1]
tab diff
keep if diff==. | diff>14 & diff!=.

rename gp mild
rename hes severe

*make a mild and severe variable
keep nhs_number_deid mild severe
sort nhs_number_deid
by nhs_number_deid: gen tot_mild=sum(mild) if mild!=.
by nhs_number_deid: gen tot_severe=sum(severe) if severe!=.
by nhs_number_deid: egen tot_mild2=max(tot_mild)
by nhs_number_deid: egen tot_severe2=max(tot_severe)
recode tot_mild2 .=0
recode tot_severe2 .=0
drop mild severe tot_mild tot_severe
duplicates drop

*now combine mild and severe with the number of exacerbations
gen tot_copd_exac=1 if tot_mild2==1 & tot_severe2==0
replace tot_copd_exac=2 if tot_mild2==2 & tot_severe2==0
replace tot_copd_exac=3 if tot_mild2>=3 & tot_severe2==0
replace tot_copd_exac=4 if tot_severe2==1
replace tot_copd_exac=5 if tot_severe2>=2
label define  lab2 1"1 mild, 0 severe" 2"2 mild, 0 severe" 3"3+ mild, 0 severe" 4"1 severe, any mild" 5"2+ severe, any mild"
label values tot_copd_exac lab2

*keep final variable and save
keep nhs_number_deid tot_copd_exac 
duplicates drop
save "D:\Users\h.whittaker\Documents\copd_exacerbations_final.dta", replace


********************************
*First COPD date
************************************
use "D:\Users\h.whittaker\Documents\single_covariates.dta", clear
keep nhs_number_deid first_copd
keep if first_copd!=.
save "D:\Users\h.whittaker\Documents\copd_date.dta", replace


*******************************************************************************************************
*Merge together 

use "ccu035_01_final_analysis_cohort_2.dta", clear
rename nhs_number_deid nhs_number_deid
merge 1:1 nhs_number_deid using "D:\Users\h.whittaker\Documents\asthma_control_final.dta"
keep if _m==3 | _m==1 
drop _m
merge 1:1 nhs_number_deid using "D:\Users\h.whittaker\Documents\asthma_exacerbations_final.dta" 
keep if _m==3 | _m==1 
drop _m
merge 1:1 nhs_number_deid using "D:\Users\h.whittaker\Documents\asthma_review_final.dta"
keep if _m==3 | _m==1 
drop _m
merge 1:1 nhs_number_deid using "D:\Users\h.whittaker\Documents\asthma_med_check_final.dta" 
keep if _m==3 | _m==1 
drop _m
merge 1:1 nhs_number_deid using "D:\Users\h.whittaker\Documents\rhinitis_final.dta" 
keep if _m==3 | _m==1 
drop _m
merge 1:1 nhs_number_deid using "D:\Users\h.whittaker\Documents\ics_use_final.dta" 
keep if _m==3 | _m==1 
drop _m
merge 1:1 nhs_number_deid using "D:\Users\h.whittaker\Documents\saba_sama_use_final.dta"
keep if _m==3 | _m==1 
drop _m
merge 1:1 nhs_number_deid using "D:\Users\h.whittaker\Documents\height_final.dta"
keep if _m==3 | _m==1 
drop _m
merge 1:1 nhs_number_deid using "D:\Users\h.whittaker\Documents\mrc_final.dta"
keep if _m==3 | _m==1 
drop _m
merge 1:1 nhs_number_deid using "D:\Users\h.whittaker\Documents\gord_final.dta"
keep if _m==3 | _m==1 
drop _m
merge 1:1 nhs_number_deid using "D:\Users\h.whittaker\Documents\copd_exacerbations_final.dta"
keep if _m==3 | _m==1 
drop _m
merge 1:1 nhs_number_deid using "D:\Users\h.whittaker\Documents\copd_date.dta"
keep if _m==3 | _m==1 
drop _m
save "ccu035_01_copd_asthma_analysis_cohort.dta", replace

*COPD duration
gen copd_duration=covid19_confirmed_date-first_copd
replace copd_duration=copd_duration/365.25
save "ccu035_01_copd_asthma_analysis_cohort.dta", replace



*LEft to create in Databricks: GINA step ->
*how to extra the ICS drug strenght from this "5mg xxxx/500mg"??? 
*Working with Tom. 


odbc load, exec ("SELECT * FROM dars_nic_391419_j3w9t_collab.ccu035_01_ics_for_gina") dsn("databricks")








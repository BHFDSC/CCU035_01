*COPD analysis
cd "D:\Users\c.kallis\Documents"

*Exposure=COPD maintenance therapy 
*Exclude people with MI, stroke, or VTE prior to covid-and as sensitivity analysis
*Outcome=nb not all the same as previous ones
********************************************************
use "ccu035_01_copd_asthma_analysis_cohort.dta", clear
keep if copd==1 // 83,514 

recode ics_use .=0

gen comp_art=1 if dic_thromb==1 | all_stroke==1 | ami_out==1
recode comp_art .=0

gen comp_ven=1 if vt_artd==1 | pe_oth_art==1
recode comp_ven .=0 

gen comp_other=1 if hf_cardio==1 | ang_unang==1 | myo_peri==1 | life_arrhythmia_out==1
recode comp_other .=0

tab comp_art
tab comp_ven
tab comp_other
tab all_cvd

tab sama_saba
recode sama_saba .=0

sum height
replace height=height*100 if height<100
replace height=. if height>300 | height<100

tab mrc

tab gord
recode gord .=0

tab tot_copd_exac
recode tot_copd_exac .=0

sum copd_duration
replace first_copd=covid19_confirmed_date if first_copd==.
drop copd_duration 
gen copd_duration=covid19_confirmed_date-first_copd
replace copd_duration=copd_duration/365.25
gen copd_duration2=1 if copd_duration<=5
replace copd_duration2=2 if copd_duration>5 & copd_duration<=10 
replace copd_duration2=3 if copd_duration>10 & copd_duration<=20
replace copd_duration2=4 if copd_duration>20
label define lab_duration 1"5 years or less" 2"6-10 years" 3"11-20 years" 4"More than 20 years"
label values copd_duration2 lab_duration


	
tab region_name,gen(region_)

ren region_1 east_midlands
ren region_2 east_of_england
ren region_3 london
ren region_4 north_east
ren region_5 north_west
ren region_6 south_east
ren region_7 south_west
ren region_8 west_midlands
ren region_9 yorkshire_and_the_humber

tab deci_imd, gen(deci_imd)

tab categorised_ethnicity,m

replace categorised_ethnicity = "" if categorised_ethnicity =="Unknown"

tab categorised_ethnicity, gen(ethnic_cat)

ren ethnic_cat1 asian

ren ethnic_cat2 black

ren ethnic_cat3 mixed

ren ethnic_cat4 other

ren ethnic_cat5 white


tab mrc,gen(mrc_)

tab tot_copd_exac, gen(tot_copd_exac_)

tab copd_duration2, gen(copd_duration2_)

save "ccu035_03_copd_analysis_cohort.dta", replace

******************************
*Cox regression 
*************************************
global log_files "D:\Users\c.kallis\Documents\"

log using "ccu035_03_ics_copd.doc", text replace

use "ccu035_03_copd_analysis_cohort.dta", clear

* p-values with 4 decimal places to apply corrections for multiple testing

set pformat %5.4f

set seed 1234

*Rates and adjusted Cox regressions
file open covid_di using "cox_cluster_estimates_ccu035_03_ics_copd.csv", write replace 
file write covid_di "Outcome" "," "HR unadjusted" "," "HR LL" "," "HR UL" "," "p-value" "," "HR adjusted" "," "HR LL" "," "HR UL" "," "p-value" "," _n
file close covid_di
	foreach var of varlist all_cvd comp_art comp_ven comp_other {
	use "ccu035_03_copd_analysis_cohort.dta", clear 
	capture drop pred1
	gen pred1=ics_use
	gen `var'_cens = 0
	replace `var'_cens = 1 if record_date != . & `var' == 1
	gen `var'_days = record_date - covid19_confirmed_date if `var'_cens == 1
	replace `var'_days = end_date - covid19_confirmed_date if `var'_cens == 0
	stset `var'_days  , failure(`var'_cens) scale(7)
	capture noisily stcox pred1, cl(practice) // add covariates  
	if _rc != 0 continue
	gen one=exp(_b[pred1])
	gen two=exp(_b[pred1] - abs(invnormal(0.025))*_se[pred1])
	gen three=exp(_b[pred1] + abs(invnormal(0.025))*_se[pred1])
	gen four=2*normal(-abs(_b[pred1]/_se[pred1]))
	capture noisily stcox pred1 age_at_cohort_start female current_smoker ex_smoker obese_bmi surgery_lastyr_hes hypertension diabetes cancer liver_disease dementia ckd depression hist_cvd cvd_meds east_midlands east_of_england north_east north_west south_east south_west west_midlands yorkshire_and_the_humber deci_imd1 deci_imd2 deci_imd3 deci_imd4 deci_imd5 deci_imd6 deci_imd7 deci_imd8 deci_imd9 asian black mixed other sama_saba height gord mrc_2 mrc_3 mrc_4 mrc_5 tot_copd_exac_2 tot_copd_exac_3 tot_copd_exac_4 tot_copd_exac_5 tot_copd_exac_6 copd_duration2_2 copd_duration2_3 copd_duration2_4 , cl(practice) 
	if _rc != 0 continue
	gen five=exp(_b[pred1])
	gen six=exp(_b[pred1] - abs(invnormal(0.025))*_se[pred1])
	gen seven=exp(_b[pred1] + abs(invnormal(0.025))*_se[pred1])
	gen eight=2*normal(-abs(_b[pred1]/_se[pred1]))
	file open desc using "cox_cluster_estimates_ccu035_03_ics_copd.csv",  write append
	file write desc "_`var'" ","  %50.2f (one) "," %50.2f (two) ","  %50.2f (three) "," %50.4f (four) "," %50.2f (five) "," %50.2f (six) ","  %50.2f (seven) "," %50.4f (eight) "," _n
	file close desc
	capture drop one two three four five six seven eight pred1 
	}

use "ccu035_03_copd_analysis_cohort.dta", clear 

log close


******************************
*Cox regression 
*************************************
global log_files "D:\Users\c.kallis\Documents\"

log using "ccu035_03_ics_copd_nocvd.doc", text replace

use "ccu035_03_copd_analysis_cohort.dta", clear

* p-values with 4 decimal places to apply corrections for multiple testing

set pformat %5.4f

set seed 1234

*Rates and adjusted Cox regressions
file open covid_di using "cox_cluster_estimates_ccu035_03_ics_copd_nocvd.csv", write replace 
file write covid_di "Outcome" "," "HR unadjusted" "," "HR LL" "," "HR UL" "," "p-value" "," "HR adjusted" "," "HR LL" "," "HR UL" "," "p-value" "," _n
file close covid_di
	foreach var of varlist all_cvd comp_art comp_ven comp_other {
	use "ccu035_03_copd_analysis_cohort.dta", clear 
	keep if vt_artd==0 & all_stroke==0 & ami_out==0
	capture drop pred1
	gen pred1=ics_use
	gen `var'_cens = 0
	replace `var'_cens = 1 if record_date != . & `var' == 1
	gen `var'_days = record_date - covid19_confirmed_date if `var'_cens == 1
	replace `var'_days = end_date - covid19_confirmed_date if `var'_cens == 0
	stset `var'_days  , failure(`var'_cens) scale(7)
	capture noisily stcox pred1, cl(practice) // add covariates  
	if _rc != 0 continue
	gen one=exp(_b[pred1])
	gen two=exp(_b[pred1] - abs(invnormal(0.025))*_se[pred1])
	gen three=exp(_b[pred1] + abs(invnormal(0.025))*_se[pred1])
	gen four=2*normal(-abs(_b[pred1]/_se[pred1]))
	capture noisily stcox pred1 age_at_cohort_start female current_smoker ex_smoker obese_bmi surgery_lastyr_hes hypertension diabetes cancer liver_disease dementia ckd depression hist_cvd cvd_meds east_midlands east_of_england north_east north_west south_east south_west west_midlands yorkshire_and_the_humber deci_imd1 deci_imd2 deci_imd3 deci_imd4 deci_imd5 deci_imd6 deci_imd7 deci_imd8 deci_imd9 asian black mixed other sama_saba height gord mrc_2 mrc_3 mrc_4 mrc_5 tot_copd_exac_2 tot_copd_exac_3 tot_copd_exac_4 tot_copd_exac_5 tot_copd_exac_6 copd_duration2_2 copd_duration2_3 copd_duration2_4 , cl(practice) 
	if _rc != 0 continue
	gen five=exp(_b[pred1])
	gen six=exp(_b[pred1] - abs(invnormal(0.025))*_se[pred1])
	gen seven=exp(_b[pred1] + abs(invnormal(0.025))*_se[pred1])
	gen eight=2*normal(-abs(_b[pred1]/_se[pred1]))
	file open desc using "cox_cluster_estimates_ccu035_03_ics_copd_nocvd.csv",  write append
	file write desc "_`var'" ","  %50.2f (one) "," %50.2f (two) ","  %50.2f (three) "," %50.4f (four) "," %50.2f (five) "," %50.2f (six) ","  %50.2f (seven) "," %50.4f (eight) "," _n
	file close desc
	capture drop one two three four five six seven eight pred1 
	}


macro drop _all

clear

log close
/*Export rates


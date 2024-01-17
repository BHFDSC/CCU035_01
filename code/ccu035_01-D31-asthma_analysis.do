*asthma analysis
cd "D:\Users\c.kallis\Documents"

*Exposure=asthma maintenance therapy 
*Exclude people with MI, stroke, or VTE prior to covid-and as sensitivity analysis
*Outcome=nb not all the same as previous ones
********************************************************
use "ccu035_01_copd_asthma_analysis_cohort.dta", clear
keep if asthma==1 // 83,514 

recode ics_24h_use .=0

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

replace tot_asthma_exac = 0 if tot_asthma_exac == .

tab tot_asthma_exac, gen(tot_asthma_exac_)


* binary exposure

gen tot_asthma_exacerbations = 0

replace tot_asthma_exacerbations = 1 if tot_asthma_exac > 0 & tot_asthma_exac!=.


replace poor_asthma_control = 0 if poor_asthma_control == .

tab gord
recode gord .=0

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



save "ccu035_03_asthma_analysis_cohort.dta", replace

******************************
*Cox regression 
*************************************
global log_files "D:\Users\c.kallis\Documents\"

log using "ccu035_03_asthma_analysis.doc", text replace

use "ccu035_03_asthma_analysis_cohort.dta", clear

* p-values with 4 decimal places to apply corrections for multiple testing

set pformat %5.4f

set seed 1234

*Rates and adjusted Cox regressions
file open covid_di using "cox_cluster_estimates_ccu035_03_asthma_analysis.csv", write replace 
file write covid_di "Outcome" "," "HR ICS unadj" "," "HR LL" "," "HR UL" "," "p-value" "," "HR poor control unadj" "," "HR LL" "," "HR UL" "," "p-value" "," "HR exacerbations unadj" "," "HR LL" "," "HR UL" "," "p-value" "," "HR ICS adj" "," "HR LL" "," "HR UL" "," "p-value" "," "HR poor control adj" "," "HR LL" "," "HR UL" "," "p-value" "," "HR exacerbations adj" "," "HR LL" "," "HR UL" "," "p-value" "," _n
file close covid_di
	foreach var of varlist all_cvd comp_art comp_ven comp_other {
	use "ccu035_03_asthma_analysis_cohort.dta", clear 
	capture drop pred1
	gen pred1=ics_24h_use
	gen `var'_cens = 0
	replace `var'_cens = 1 if record_date != . & `var' == 1
	gen `var'_days = record_date - covid19_confirmed_date if `var'_cens == 1
	replace `var'_days = end_date - covid19_confirmed_date if `var'_cens == 0
	stset `var'_days  , failure(`var'_cens) scale(7)
	capture noisily stcox pred1, cl(practice) //   
	if _rc != 0 continue
	gen one=exp(_b[pred1])
	gen two=exp(_b[pred1] - abs(invnormal(0.025))*_se[pred1])
	gen three=exp(_b[pred1] + abs(invnormal(0.025))*_se[pred1])
	gen four=2*normal(-abs(_b[pred1]/_se[pred1]))
	capture drop pred1
	gen pred1=poor_asthma_control
	capture noisily stcox pred1, cl(practice) //   
	if _rc != 0 continue
	gen five=exp(_b[pred1])
	gen six=exp(_b[pred1] - abs(invnormal(0.025))*_se[pred1])
	gen seven=exp(_b[pred1] + abs(invnormal(0.025))*_se[pred1])
	gen eight=2*normal(-abs(_b[pred1]/_se[pred1]))
	capture drop pred1
	gen pred1=tot_asthma_exacerbations
	capture noisily stcox pred1, cl(practice) //  
	if _rc != 0 continue
	gen nine=exp(_b[pred1])
	gen ten=exp(_b[pred1] - abs(invnormal(0.025))*_se[pred1])
	gen eleven=exp(_b[pred1] + abs(invnormal(0.025))*_se[pred1])
	gen twelve=2*normal(-abs(_b[pred1]/_se[pred1]))
	capture drop pred1
	gen pred1=ics_24h_use 
	gen pred2=poor_asthma_control
	gen pred3=tot_asthma_exacerbations
	capture noisily stcox pred1 pred2 pred3 age_at_cohort_start female current_smoker ex_smoker obese_bmi surgery_lastyr_hes hypertension diabetes cancer liver_disease dementia ckd depression hist_cvd cvd_meds east_midlands east_of_england north_east north_west south_east south_west west_midlands yorkshire_and_the_humber deci_imd1 deci_imd2 deci_imd3 deci_imd4 deci_imd5 deci_imd6 deci_imd7 deci_imd8 deci_imd9 asian black mixed other sama_saba gord, cl(practice) // 
	if _rc != 0 continue
	gen thirteen=exp(_b[pred1])
	gen fourteen=exp(_b[pred1] - abs(invnormal(0.025))*_se[pred1])
	gen fifteen=exp(_b[pred1] + abs(invnormal(0.025))*_se[pred1])
	gen sixteen=2*normal(-abs(_b[pred1]/_se[pred1]))
	gen seventeen=exp(_b[pred2])
	gen eighteen=exp(_b[pred2] - abs(invnormal(0.025))*_se[pred2])
	gen nineteen=exp(_b[pred2] + abs(invnormal(0.025))*_se[pred2])
	gen twenty=2*normal(-abs(_b[pred2]/_se[pred2]))
	gen twentyone=exp(_b[pred3])
	gen twentytwo=exp(_b[pred3] - abs(invnormal(0.025))*_se[pred3])
	gen twentythree=exp(_b[pred3] + abs(invnormal(0.025))*_se[pred3])
	gen twentyfour=2*normal(-abs(_b[pred3]/_se[pred3]))
	file open desc using "cox_cluster_estimates_ccu035_03_asthma_analysis.csv",  write append
	file write desc "_`var'" ","  %50.2f (one) "," %50.2f (two) ","  %50.2f (three) "," %50.4f (four) "," %50.2f (five) "," %50.2f (six) ","  %50.2f (seven) "," %50.4f (eight) "," %50.2f (nine) "," %50.2f (ten) ","  %50.2f (eleven) "," %50.4f (twelve) "," %50.2f (thirteen) "," %50.2f (fourteen) ","  %50.2f (fifteen) "," %50.4f (sixteen) "," %50.2f (seventeen) "," %50.2f (eighteen) ","  %50.2f (nineteen) "," %50.4f (twenty) "," %50.2f (twentyone) ","  %50.2f (twentytwo) "," %50.2f (twentythree) "," %50.4f (twentyfour) ","_n
	file close desc
	capture drop one two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen sixteen seventeen eighteen nineteen twenty twentyone twentytwo twentythree twentyfour pred1 pred2 pred3
	}

use "ccu035_03_asthma_analysis_cohort.dta", clear 

log close


******************************
*Cox regression 
*************************************
global log_files "D:\Users\c.kallis\Documents\"

log using "ccu035_03_ics_nocvd.doc", text replace

use"ccu035_03_asthma_analysis_cohort.dta", clear

* p-values with 4 decimal places to apply corrections for multiple testing

set pformat %5.4f

set seed 1234

*Rates and adjusted Cox regressions
file open covid_di using "cox_cluster_estimates_ccu035_03_asthma_analysis_nocvd.csv", write replace 
file write covid_di "Outcome" "," "HR ICS unadj" "," "HR LL" "," "HR UL" "," "p-value" "," "HR poor control unadj" "," "HR LL" "," "HR UL" "," "p-value" "," "HR exacerbations unadj" "," "HR LL" "," "HR UL" "," "p-value" "," "HR ICS adj" "," "HR LL" "," "HR UL" "," "p-value" "," "HR poor control adj" "," "HR LL" "," "HR UL" "," "p-value" "," "HR exacerbations adj" "," "HR LL" "," "HR UL" "," "p-value" "," _n
file close covid_di
	foreach var of varlist all_cvd comp_art comp_ven comp_other {
	use "ccu035_03_asthma_analysis_cohort.dta", clear 
	keep if vt_artd==0 & all_stroke==0 & ami_out==0
	capture drop pred1
	gen pred1=ics_24h_use
	gen `var'_cens = 0
	replace `var'_cens = 1 if record_date != . & `var' == 1
	gen `var'_days = record_date - covid19_confirmed_date if `var'_cens == 1
	replace `var'_days = end_date - covid19_confirmed_date if `var'_cens == 0
	stset `var'_days  , failure(`var'_cens) scale(7)
	capture noisily stcox pred1, cl(practice) //   
	if _rc != 0 continue
	gen one=exp(_b[pred1])
	gen two=exp(_b[pred1] - abs(invnormal(0.025))*_se[pred1])
	gen three=exp(_b[pred1] + abs(invnormal(0.025))*_se[pred1])
	gen four=2*normal(-abs(_b[pred1]/_se[pred1]))
	capture drop pred1
	gen pred1=poor_asthma_control
	capture noisily stcox pred1, cl(practice) // 
	if _rc != 0 continue
	gen five=exp(_b[pred1])
	gen six=exp(_b[pred1] - abs(invnormal(0.025))*_se[pred1])
	gen seven=exp(_b[pred1] + abs(invnormal(0.025))*_se[pred1])
	gen eight=2*normal(-abs(_b[pred1]/_se[pred1]))
	capture drop pred1
	gen pred1=tot_asthma_exacerbations
	capture noisily stcox pred1, cl(practice) //
	if _rc != 0 continue
	gen nine=exp(_b[pred1])
	gen ten=exp(_b[pred1] - abs(invnormal(0.025))*_se[pred1])
	gen eleven=exp(_b[pred1] + abs(invnormal(0.025))*_se[pred1])
	gen twelve=2*normal(-abs(_b[pred1]/_se[pred1]))
	capture drop pred1
	gen pred1=ics_24h_use 
	gen pred2=poor_asthma_control
	gen pred3=tot_asthma_exacerbations
	capture noisily stcox pred1 pred2 pred3 age_at_cohort_start female current_smoker ex_smoker obese_bmi surgery_lastyr_hes hypertension diabetes cancer liver_disease dementia ckd depression hist_cvd cvd_meds east_midlands east_of_england north_east north_west south_east south_west west_midlands yorkshire_and_the_humber deci_imd1 deci_imd2 deci_imd3 deci_imd4 deci_imd5 deci_imd6 deci_imd7 deci_imd8 deci_imd9 asian black mixed other sama_saba gord, cl(practice) //
	if _rc != 0 continue
	gen thirteen=exp(_b[pred1])
	gen fourteen=exp(_b[pred1] - abs(invnormal(0.025))*_se[pred1])
	gen fifteen=exp(_b[pred1] + abs(invnormal(0.025))*_se[pred1])
	gen sixteen=2*normal(-abs(_b[pred1]/_se[pred1]))
	gen seventeen=exp(_b[pred2])
	gen eighteen=exp(_b[pred2] - abs(invnormal(0.025))*_se[pred2])
	gen nineteen=exp(_b[pred2] + abs(invnormal(0.025))*_se[pred2])
	gen twenty=2*normal(-abs(_b[pred2]/_se[pred2]))
	gen twentyone=exp(_b[pred3])
	gen twentytwo=exp(_b[pred3] - abs(invnormal(0.025))*_se[pred3])
	gen twentythree=exp(_b[pred3] + abs(invnormal(0.025))*_se[pred3])
	gen twentyfour=2*normal(-abs(_b[pred3]/_se[pred3]))
	file open desc using "cox_cluster_estimates_ccu035_03_asthma_analysis_nocvd.csv",  write append
	file write desc "_`var'" ","  %50.2f (one) "," %50.2f (two) ","  %50.2f (three) "," %50.4f (four) "," %50.2f (five) "," %50.2f (six) ","  %50.2f (seven) "," %50.4f (eight) "," %50.2f (nine) "," %50.2f (ten) ","  %50.2f (eleven) "," %50.4f (twelve) "," %50.2f (thirteen) "," %50.2f (fourteen) ","  %50.2f (fifteen) "," %50.4f (sixteen) "," %50.2f (seventeen) "," %50.2f (eighteen) ","  %50.2f (nineteen) "," %50.4f (twenty) "," %50.2f (twentyone) ","  %50.2f (twentytwo) "," %50.2f (twentythree) "," %50.4f (twentyfour) ","_n
	file close desc
	capture drop one two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen sixteen seventeen eighteen nineteen twenty twentyone twentytwo twentythree twentyfour pred1 
	}

macro drop _all

clear

log close
/*Export rates


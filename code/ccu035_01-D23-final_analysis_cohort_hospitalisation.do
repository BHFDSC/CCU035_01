clear

set more off

macro drop _all

capture log close 


global log_files "D:\Users\c.kallis\Documents\"

log using "${log_files}ccu035_01_final_analysis_cohort2_hospitalisation.doc", text replace


global log_files "D:\Users\c.kallis\Documents\"

cd "D:\Users\c.kallis\Documents"

clear



use "ccu035_01_final_analysis_cohort_2.dta", clear

codebook nhs_number_deid


use "ccu035_01_final_analysis_cohort_2.dta", clear

* p-values with 4 decimal places to apply corrections for multiple testing

set pformat %5.4f

set seed 1234

*Rates and adjusted Cox regressions
file open covid_di using "cox_cluster_estimates_ccu035_01_hospitalisation.csv", write replace 
file write covid_di "Outcome" "," "HR 1" "," "HR LL" "," "HR UL" "," "p-value" "," "HR 2" "," "HR LL" "," "HR UL" "," "p-value" "," "HR 3" "," "HR LL" "," "HR UL" "," "p-value" "," "HR 4" "," "HR LL" "," "HR UL" "," "p-value" "," "HR 5" "," "HR LL" "," "HR UL" "," "p-value" "," _n
file close covid_di
	foreach var of varlist all_cvd vt_artd dic_thromb hf_cardio ang_unang myo_peri all_stroke ami_out pe_oth_art life_arrhythmia_out {
	use "ccu035_01_final_analysis_cohort_2.dta", clear
	
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


tab covid_hospitalisation_phenotype,gen(hospitalisation_)

ren hospitalisation_1 hospitalised

ren hospitalisation_2 not_hospitalised

	capture drop pred1
	gen pred1=resp_all
	gen pred2=hospitalised*pred1
	gen `var'_cens = 0
	replace `var'_cens = 1 if record_date != . & `var' == 1
	gen `var'_days = record_date - covid19_confirmed_date if `var'_cens == 1
	replace `var'_days = end_date - covid19_confirmed_date if `var'_cens == 0
	stset `var'_days  , failure(`var'_cens) scale(7)
	* without adjustment for hospitalisation
capture noisily stcox pred1 age_at_cohort_start female current_smoker ex_smoker obese_bmi surgery_lastyr_hes hypertension diabetes cancer liver_disease dementia ckd depression hist_cvd cvd_meds east_midlands east_of_england north_east north_west south_east south_west west_midlands yorkshire_and_the_humber deci_imd1 deci_imd2 deci_imd3 deci_imd4 deci_imd5 deci_imd6 deci_imd7 deci_imd8 deci_imd9 asian black mixed other, cl(practice)	
	if _rc != 0 continue
	gen one=exp(_b[pred1])
	gen two=exp(_b[pred1] - abs(invnormal(0.025))*_se[pred1])
	gen three=exp(_b[pred1] + abs(invnormal(0.025))*_se[pred1])
	gen four=2*normal(-abs(_b[pred1]/_se[pred1]))
	* without interaction
	capture noisily stcox pred1 hospitalised age_at_cohort_start female current_smoker ex_smoker obese_bmi surgery_lastyr_hes hypertension diabetes cancer liver_disease dementia ckd depression hist_cvd cvd_meds east_midlands east_of_england north_east north_west south_east south_west west_midlands yorkshire_and_the_humber deci_imd1 deci_imd2 deci_imd3 deci_imd4 deci_imd5 deci_imd6 deci_imd7 deci_imd8 deci_imd9 asian black mixed other, cl(practice)	
	if _rc != 0 continue
	gen five=exp(_b[pred1])
	gen six=exp(_b[pred1] - abs(invnormal(0.025))*_se[pred1])
	gen seven=exp(_b[pred1] + abs(invnormal(0.025))*_se[pred1])
	gen eight=2*normal(-abs(_b[pred1]/_se[pred1]))
	* with interaction
capture noisily stcox pred1 hospitalised pred2 age_at_cohort_start female current_smoker ex_smoker obese_bmi surgery_lastyr_hes hypertension diabetes cancer liver_disease dementia ckd depression hist_cvd cvd_meds east_midlands east_of_england north_east north_west south_east south_west west_midlands yorkshire_and_the_humber deci_imd1 deci_imd2 deci_imd3 deci_imd4 deci_imd5 deci_imd6 deci_imd7 deci_imd8 deci_imd9 asian black mixed other, cl(practice)
	if _rc != 0 continue
	gen nine=exp(_b[pred1])
	gen ten=exp(_b[pred1] - abs(invnormal(0.025))*_se[pred1])
	gen eleven=exp(_b[pred1] + abs(invnormal(0.025))*_se[pred1])
	gen twelve=2*normal(-abs(_b[pred1]/_se[pred1]))
	gen thirteen=exp(_b[pred2])
	gen fourteen=exp(_b[pred2] - abs(invnormal(0.025))*_se[pred2])
	gen fifteen=exp(_b[pred2] + abs(invnormal(0.025))*_se[pred2])
	gen sixteen=2*normal(-abs(_b[pred2]/_se[pred2]))
lincom pred1 + pred2
	gen seventeen=exp(r(estimate))
	gen eighteen=exp(r(lb))
	gen nineteen=exp(r(ub))
	gen twenty=r(p)
	file open desc using "cox_cluster_estimates_ccu035_01_hospitalisation.csv",  write append
	file write desc "_`var'" ","  %50.2f (one) "," %50.2f (two) ","  %50.2f (three) "," %50.4f (four) "," %50.2f (five) "," %50.2f (six) ","  %50.2f (seven) "," %50.4f (eight) "," %50.2f (nine) "," %50.2f (ten) ","  %50.2f (eleven) "," %50.4f (twelve) "," %50.2f (thirteen) "," %50.2f (fourteen) ","  %50.2f (fifteen) "," %50.4f (sixteen) "," %50.2f (seventeen) "," %50.2f (eighteen) ","  %50.2f (nineteen) "," %50.4f (twenty) ","_n
	file close desc
	capture drop one two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen sixteen seventeen eighteen nineteen twenty pred1 pred2
	}

use "ccu035_01_final_analysis_cohort_2.dta", clear


macro drop _all

clear

log close

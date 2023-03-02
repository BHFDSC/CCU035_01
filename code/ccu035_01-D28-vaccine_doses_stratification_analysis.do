clear

set more off

macro drop _all

capture log close 


global log_files "D:\Users\c.kallis\Documents\"

log using "${log_files}ccu035_01_vaccine_doses_all_cvd_resp_all_stratification analysis.doc", text replace


global log_files "D:\Users\c.kallis\Documents\"

cd "D:\Users\c.kallis\Documents"

clear

forval i=0(1)1 {

use "vaccine_analysis_table2.dta", clear 

* by respiratory disease status

keep if resp_all == `i'

drop if start_date > record_date & start_date!=. & record_date!=. & all_cvd==1

tab resp_all

gen all_cvd_cens = 0
	replace all_cvd_cens = 1 if record_date != . & record_date>= start_date & record_date <= stop_date & all_cvd == 1
	gen all_cvd_days = record_date - start_date if all_cvd_cens == 1
	replace all_cvd_days = stop_date - start_date if all_cvd_cens == 0
	replace all_cvd_days = 0.5 if all_cvd_days < 0
	replace all_cvd_days = 0.5 if all_cvd_days == 0
	
stset all_cvd_days  , failure(all_cvd_cens) scale(7)
	
tab vaccine_number2, gen(vacc_num_)
ren vacc_num_1 unvaccinated
ren vacc_num_2 dose_1
ren vacc_num_3 dose_2
ren vacc_num_4 booster

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


codebook nhs_number_deid

tab resp_all


stcox dose_1 dose_2 booster, cluster(nhs_number_deid)

stcox dose_1 dose_2 booster age_at_cohort_start female current_smoker ex_smoker obese_bmi surgery_lastyr_hes hypertension diabetes cancer liver_disease dementia ckd depression hist_cvd cvd_meds east_midlands east_of_england north_east north_west south_east south_west west_midlands yorkshire_and_the_humber deci_imd1 deci_imd2 deci_imd3 deci_imd4 deci_imd5 deci_imd6 deci_imd7 deci_imd8 deci_imd9 asian black mixed other, cluster(nhs_number_deid)

}



use "vaccine_analysis_table2.dta", clear 


macro drop _all

clear

log close

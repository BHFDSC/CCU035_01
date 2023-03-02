clear

set more off

macro drop _all

capture log close 


global log_files "D:\Users\c.kallis\Documents\"

log using "${log_files}ccu035_01_final_analysis_cohort2_desc2.doc", text replace


global log_files "D:\Users\c.kallis\Documents\"

cd "D:\Users\c.kallis\Documents"

clear


* CVD events (and each CVD component) by baseline respiratory group

use "ccu035_01_final_analysis_cohort_2.dta", clear

codebook nhs_number_deid
 
foreach var of varlist all_cvd vt_artd dic_thromb hf_cardio ang_unang myo_peri all_stroke ami_out pe_oth_art life_arrhythmia_out {

tab `var' resp_all, col

}

* CVD events (and each CVD component) by baseline respiratory group in those with no history of CVD

use "ccu035_01_final_analysis_cohort_2.dta", clear

drop if ami == 1 | vt == 1 | dvt_icvt == 1 | artery_dissect == 1 | stroke_isch == 1 | stroke_sah_hs == 1 | stroke_tia == 1

codebook nhs_number_deid

foreach var of varlist all_cvd vt_artd dic_thromb hf_cardio ang_unang myo_peri all_stroke ami_out pe_oth_art life_arrhythmia_out {

tab `var' resp_all, col

}


* CVD events (and each CVD component) by baseline respiratory group in those with no history of CVD and who were hospitalised with COVID

use "ccu035_01_final_analysis_cohort_2.dta", clear

drop if ami == 1 | vt == 1 | dvt_icvt == 1 | artery_dissect == 1 | stroke_isch == 1 | stroke_sah_hs == 1 | stroke_tia == 1

tab covid_hospitalisation_phenotype,gen(hospitalisation_)

ren hospitalisation_1 hospitalised

ren hospitalisation_2 not_hospitalised

keep if hospitalised == 1

codebook nhs_number_deid


foreach var of varlist all_cvd vt_artd dic_thromb hf_cardio ang_unang myo_peri all_stroke ami_out pe_oth_art life_arrhythmia_out {

tab `var' resp_all, col

}
* CVD events (and each CVD component) by baseline respiratory group in those with no history of CVD and who were not hospitalised with COVID

use "ccu035_01_final_analysis_cohort_2.dta", clear

drop if ami == 1 | vt == 1 | dvt_icvt == 1 | artery_dissect == 1 | stroke_isch == 1 | stroke_sah_hs == 1 | stroke_tia == 1

tab covid_hospitalisation_phenotype,gen(hospitalisation_)

ren hospitalisation_1 hospitalised

ren hospitalisation_2 not_hospitalised

keep if not_hospitalised == 1

codebook nhs_number_deid

foreach var of varlist all_cvd vt_artd dic_thromb hf_cardio ang_unang myo_peri all_stroke ami_out pe_oth_art life_arrhythmia_out {

tab `var' resp_all, col

}

macro drop _all

clear

log close

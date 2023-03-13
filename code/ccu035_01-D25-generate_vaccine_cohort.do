clear

set more off

macro drop _all

capture log close 


global log_files "D:\Users\c.kallis\Documents\"

log using "${log_files}ccu035_01_vaccine_doses.doc", text replace


global log_files "D:\Users\c.kallis\Documents\"

cd "D:\Users\c.kallis\Documents"

clear



*1) Formatting vaccine doses 
clear all
odbc load, exec ("SELECT * FROM dars_nic_391419_j3w9t_collab.ccu035_01_vaccine_analysis_dates") dsn("databricks")

rename *, lower

rename vaccination_date vaccine_date 

sort nhs_number_deid vaccine_date vaccine_dose 
duplicates drop

gen mrna=0 if vaccine_product=="AstraZeneca"
replace mrna=1 if vaccine_product=="Pfizer" | vaccine_product=="Moderna"

gen lcd  = date("30/11/2021", "DMY") 
format lcd %td

drop if vaccine_date>lcd
drop lcd
save "vaccine_doses.dta", replace 

*2) Sorting out CVD outcome data
clear all
odbc load, exec ("SELECT * FROM dars_nic_391419_j3w9t_collab.ccu035_01_cvdoutcomes_first_all") dsn("databricks")

rename *, lower

sort nhs_number_deid record_date
drop end_date 
by nhs_number_deid: gen litn=_n
keep if litn==1
drop litn
codebook nhs_number_deid
drop source terminology code seq covid19_confirmed_date
save "vaccine_outcome_data.dta", replace 


*3) Keep neccesary variables from main analysis table
use "ccu035_01_final_analysis_cohort_2.dta", clear

drop all_cvd vt_artd dic_thromb hf_cardio ang_unang myo_peri all_stroke ami_out pe_oth_art life_arrhythmia_out


rename *, lower

*drop outcome events from this table 
drop record_date name term source terminology code seq end_date fracture fracture_date litn
merge 1:1 nhs_number_deid using "vaccine_outcome_data.dta"
keep if _m==3 | _m==1
drop _m

*generate end date= death or end of collection (30/11/2021)
*gen lcd  = date("30/11/2021", "DMY") 
*format lcd %td

* gen start and end dates for each patient
gen end_date=min(lcd, date_of_death)
format end_date %td

*4) Merge in vaccine data 
merge 1:m nhs_number_deid using "vaccine_doses.dta"
keep if _m==3 | _m==1
drop _m

sort nhs_number_deid vaccine_dose 

gen all_cvd = 0

replace all_cvd = 1 if record_date != .

/*

email from Hannah 1/6/22

Chatted to Jenni and she recommended combing the following outcome groupings: 

*/

* 1)	All the DVT ones and the portal vein thrombosis one and the artery dissect one.

gen vt_artd = 0

replace vt_artd = 1 if name=="DVT_DVT" | name == "DVT_ICVT" | name == "other_DVT"| name == "portal_vein_thrombosis" | name == "artery_dissect"

tab vt_artd

* 2)	DIC and TTP and thrombocytopenia + thrombophilia + mesenteric thrombus 

gen dic_thromb = 0

replace dic_thromb = 1 if name == "DIC" | name == "TTP" | name == "thrombocytopenia" | name == "thrombophilia" | name == "mesenteric_thrombus" 

tab dic_thromb

* 3)	HF and cardiomyopathy 

gen hf_cardio = 0

replace hf_cardio = 1 if name =="HF" | name == "cardiomyopathy"

tab hf_cardio

* 4)	Angina and unstable angina 

gen ang_unang = 0

replace ang_unang = 1 if name == "angina" | name == "unstable_angina"

tab ang_unang

* 5)	Myocarditis and pericarditis

gen myo_peri = 0

replace myo_peri = 1 if name == "myocarditis" | name =="pericarditis"

tab myo_peri
 
* 6)	All the strokes together

gen all_stroke = 0

replace all_stroke = 1 if name == "stroke_SAH_HS" | name == "stroke_TIA" | name == "stroke_isch" 

tab all_stroke

* 7)	AMI on its own

gen ami_out = 0

replace ami_out = 1 if name == "AMI"

tab ami_out
 
* 8)	PE and other arterial embolism  

gen pe_oth_art = 0

replace pe_oth_art = 1 if name == "PE" | name == "other_arterial_embolism"

tab pe_oth_art

* 9)	Arrythmia on its own 

gen life_arrhythmia_out = 0

replace life_arrhythmia_out = 1 if name == "life_arrhythmia"

tab life_arrhythmia_out


save "vaccine_analysis_table.dta", replace 




use "vaccine_analysis_table.dta", clear 

sort nhs_number_deid vaccine_date

by nhs_number_deid: gen litn=_n

rename litn vaccine_number

label define vaccine_numberc 1 "Dose 1" 2 "Dose 2" 3 "Booster"

label values vaccine_number vaccine_numberc

tab vaccine_number

tab vaccine_number, nol

sort nhs_number_deid vaccine_date vaccine_number

bro nhs_number_deid vaccine_date vaccine_number 

by nhs_number_deid: gen vaccine_date_1 = vaccine_date[_n-1]
format vaccine_date_1 %td

codebook nhs_number_deid

drop if vaccine_date_1 == vaccine_date & vaccine_date_1!=. & vaccine_date ! = .

drop vaccine_date_1

codebook nhs_number_deid



replace record_date=. if record_date>end_date & record_date!=. 

replace vaccine_date=. if vaccine_date>end_date & vaccine_date!=. 

drop if vaccine_date == . & vaccine_number!=.

drop if vaccine_date < covid19_confirmed_date & covid19_confirmed_date!=. & vaccine_date!=. 



expand 2 if vaccine_number==1

save "vaccine_analysis_table2.dta", replace 


use "vaccine_analysis_table2.dta", clear 

sort nhs_number_deid vaccine_date vaccine_number

by nhs_number_deid: gen litn=_n

gen start_date = covid19_confirmed_date

gen stop_date = end_date

format start_date stop_date %td

gen vaccine_number2 = litn - 1

label define vaccine_number2c 0 "Unvaccinated" 1 "Dose 1" 2 "Dose 2" 3 "Booster"

label values vaccine_number2 vaccine_number2c

tab vaccine_number2

tab vaccine_number2, nol

by nhs_number_deid: replace start_date = vaccine_date if litn > 1

by nhs_number_deid: replace stop_date = vaccine_date[_n+1] - 1 if litn < 4

replace stop_date = end_date if stop_date == .

drop litn

sort nhs_number_deid vaccine_date vaccine_number2
codebook nhs_number_deid

save "vaccine_analysis_table2.dta", replace 

use "vaccine_analysis_table2.dta", clear 




macro drop _all

clear

log close

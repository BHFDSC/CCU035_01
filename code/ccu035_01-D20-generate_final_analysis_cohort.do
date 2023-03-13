clear

set more off

macro drop _all

capture log close 


global log_files "D:\Users\c.kallis\Documents\"

log using "${log_files}ccu035_01_final_analysis_cohort2.doc", text replace


global log_files "D:\Users\c.kallis\Documents\"

cd "D:\Users\c.kallis\Documents"

clear

odbc load, exec("SELECT * FROM dars_nic_391419_j3w9t_collab.ccu035_01_start_and_end_date") dsn("databricks")

save "end_date.dta", replace

odbc load, exec("SELECT * FROM dars_nic_391419_j3w9t_collab.ccu035_01_final_analysis_cohort") dsn("databricks")

* Jenni 1/6/22

drop if name=="DVT_pregnancy"

drop end_date
merge m:1 NHS_NUMBER_DEID using "end_date.dta"
keep if _m==3
drop _m
codebook NHS_NUMBER_DEID

* email from Hannah

*4) Recoding missing values to 0 (nb: obese_bmi has a 0 already) 

foreach var of varlist BMI_obesity SURGERY_LASTYR_HES hypertension diabetes cancer liver_disease dementia CKD AMI VT DVT_ICVT PE stroke_isch stroke_SAH_HS thrombophilia TCP angina other_arterial_embolism DIC mesenteric_thrombus artery_dissect life_arrhythmia cardiomyopathy HF pericarditis myocarditis stroke_TIA antiplatelet bp_lowering lipid_lowering anticoagulant cocp hrt depression asthma ILD CF bronchiectasis {
	 
	recode `var' .=0 
	
	} 
*5) Tabulating respiratory exposure groups (add copd when Tom gets back..) 

foreach var of varlist asthma ILD CF bronchiectasis { 
    
	tab `var', miss 
	
	}
	
*6) Tabulating demographic variables 

foreach var of varlist SEX DECI_IMD region_name smoking_status { 
	
    tab `var', miss 
	
	}
	
*7) Tabulating morbidity variables 

gen hist_CVD=1 if AMI==1 | VT==1 | DVT_ICVT==1 | PE==1 | stroke_isch==1 | stroke_SAH==1 |thrombophilia==1 | TCP==1 | angina==1 | other_arterial_embolism==1 | DIC==1 | mesenteric_thrombus==1 | artery_dissect==1 | life_arrhythmia==1 | cardiomyopathy==1 | HF==1 | pericarditis==1 | myocarditis==1 | stroke_TIA==1

foreach var of varlist SURGERY_LASTYR_HES hypertension diabetes cancer liver_disease dementia CKD hist_CVD depression { 
	
	recode `var' .=0 
	
	}

foreach var of varlist OBESE_BMI SURGERY_LASTYR_HES hypertension diabetes cancer liver_disease dementia CKD hist_CVD depression {
	 
    tab `var', miss 
	
	}
	
*8) Tabulating medication variables 

gen CVD_meds=1 if antiplatelet==1 | bp_lowering==1 | lipid_lowering==1 | anticoagulant==1 | cocp==1 | hrt==1 

recode CVD_meds .=0 

tab CVD_meds, miss 


rename *, lower

*Load in the ccu035_01_final_analysis_cohort file

*drop those who have their start date after their end date
codebook nhs_number_deid if covid19_confirmed_date<end_date // 3,769,168
*if first vaccine is before covid-19 date
codebook nhs_number_deid if vaccination_date<covid19_confirmed_date & vaccination_date!=. & covid19_confirmed_date!=.
 
keep if covid19_confirmed_date<end_date
codebook nhs_number_deid // 3,769,168 


*recode fractures to another column as this is our test variable
gen fracture=1 if name=="fracture" 
gen fracture_date=record_date if fracture==1
replace name="." if fracture==1
replace record_date=. if fracture==1

*sort patid and keep first event 
sort nhs_number_deid 
by nhs_number_deid: gen litn=_n
keep if litn==1
codebook nhs_number_deid // 3,769,168


*generate resp_all variable 
gen resp_all=1 if copd==1 | asthma==1 | ild==1 | cf==1 | bronchiectasis==1
recode resp_all .=0
tab resp_all // 684,837 (18.2%)
recode copd .=0
tab copd // 86,749 (2.3)
recode asthma .=0
tab asthma // 325,515 (16.6)
recode ild .=0
tab ild // 9,397 (0.3)
recode cf .=0
tab cf // 739(0.02)
recode bronchiectasis .=0
tab bronch // 17,703 (0.5)

destring age_at_cohort_start, replace

sum age_at_cohort_start, d



tab sex, gen(sex_)

ren sex_1 male

ren sex_2 female

codebook nhs_number_deid


tab smoking_status, m

tab obese_bmi, m


tab smoking_status,gen(smoking_status_)

forvalues i=1(1)3 {
	
replace smoking_status_`i' = . if smoking_status == ""
	
}

ren smoking_status_1 current_smoker 

ren smoking_status_2 ex_smoker

ren smoking_status_3 never_smoker


save "ccu035_01_final_analysis_cohort.dta", replace

use "ccu035_01_final_analysis_cohort.dta", clear


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


codebook nhs_number_deid

codebook practice



save "ccu035_01_final_analysis_cohort_2.dta", replace



macro drop _all

clear

log close

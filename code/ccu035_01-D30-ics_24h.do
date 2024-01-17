clear

set more off

macro drop _all

capture log close 


global log_files "D:\Users\c.kallis\Documents\"

log using "${log_files}ccu035_02_ics_24h.doc", text replace


global log_files "D:\Users\c.kallis\Documents\"

cd "D:\Users\c.kallis\Documents"

clear

odbc load, exec("SELECT * FROM dars_nic_391419_j3w9t_collab.ccu035_01_ics_for_gina2") dsn("databricks")


gen dose1_mg =""

forval i=2(1)4 {
	
gen substr_`i'=substr(PrescribedMedicineStrength,`i',1)

local j = `i' - 1

replace dose1_mg = substr(PrescribedMedicineStrength,1,`j') if substr_`i'==" "

}

destring dose1_mg, replace

tab dose1_mg, m



gen dose2_mg =""

forval i=21(1)25 {
	
gen substr_`i'=substr(PrescribedMedicineStrength,`i',1)

forval k=2(1)4 {

local m = `k' + 18

local n = `i' - `m'

replace dose2_mg = substr(PrescribedMedicineStrength,`m',`n') if substr_`i'==" " & substr_`k'==" "

}

}

tab dose2_mg, m



forval i=16(1)20 {
	
gen substr_`i'=substr(PrescribedMedicineStrength,`i',1)

forval k=2(1)4 {

local m = `k' + 13

local n = `i' - `m'

replace dose2_mg = substr(PrescribedMedicineStrength,`m',`n') if substr_`i'==" " & substr_`k'==" " & dose2_mg==""

}

}


destring dose2_mg, replace

tab dose2_mg, m


gen dose3_mg =""

forval i=40(1)46 {
	
gen substr_`i'=substr(PrescribedMedicineStrength,`i',1)

forval k=21(1)25 {

local m = `k' + 18

local n = `i' - `m'

replace dose3_mg = substr(PrescribedMedicineStrength,`m',`n') if substr_`i'==" " & substr_`k'==" "

}

}

destring dose3_mg, replace

tab dose3_mg, m


drop substr_*


* Jenni 5/12/22: maximum dose corresponds to ICS dose

egen dose_mg = rowmax(dose1_mg dose2_mg dose3_mg)


drop if dose_mg < 50


rename *, lower

replace prescribedbnfname = lower(prescribedbnfname)



* use the generated dose_mg variable and multiply by "daily_dose" variable where we have it and where we don't we will assume the 24 hour dose is twice the generated dose (assumes patient is prescribed two puffs a day)
** x2 assumption is our primary analysis

gen dose_24h = .

replace dose_24h=2*dose_mg



gen ics_24h_level = .

label define ics_24h_levelc 1 "very low" 2 "low" 3 "medium" 4 "high"

label values ics_24h_level ics_24h_levelc


save "ccu035_02_ics_24h.dta", replace

use "ccu035_02_ics_24h.dta", clear


* drop Trelegy as it is only prescribed for COPD

drop if strmatch(prescribedbnfname,"*trelegy*")


codebook nhs_number_deid

gen bnfcode9 = substr(prescribedbnfcode,1,9)

merge m:1 bnfcode9 using "prescribed_meds.dta"

drop if _merge == 2

drop _merge

codebook nhs_number_deid

tab substancename, m

replace substancename=lower(substancename)


tab substancename, m


******************************************************************************  
*** ASSIGNING ics categories *** "ADULTS" ie 12 and over
******************************************************************************


** PRESSURED INHALERS
* beclometasone only inhalers
* clenil
replace ics_24h_level = 2 if dose_24h<=400 & strmatch(prescribedbnfname,"*clenil*")==1 & strmatch(substancename,"*beclometasone*")==1     // 

replace ics_24h_level = 3 if dose_24h>400 & dose_24h<=800 & strmatch(prescribedbnfname,"*clenil*")==1 & strmatch(substancename,"*beclometasone*")==1   //
** 
replace ics_24h_level = 4 if dose_24h>800 & dose_24h!=. & strmatch(prescribedbnfname,"*clenil*")==1 & strmatch(substancename,"*beclometasone*")==1    //

** kehale: no changes but scooped up later
replace ics_24h_level = 2 if dose_24h<=200 & strmatch(prescribedbnfname,"*kelhale*")==1 & strmatch(substancename,"*beclometasone*")==1   // 0

replace ics_24h_level = 3 if dose_24h>200 & dose_24h<=400 & strmatch(prescribedbnfname,"*kelhale*")==1 & strmatch(substancename,"*beclometasone*")==1   // 0

replace ics_24h_level = 4 if dose_24h>400 & dose_24h!=. & strmatch(prescribedbnfname,"*kelhale*")==1 & strmatch(substancename,"*beclometasone*")==1    // 0


** qvar:
replace ics_24h_level = 2 if dose_24h<=200 & strmatch(prescribedbnfname,"*qvar*")==1 & strmatch(substancename,"*beclometasone*")==1   // 

replace ics_24h_level = 3 if dose_24h>200 & dose_24h<=400 & strmatch(prescribedbnfname,"*qvar*")==1 & strmatch(substancename,"*beclometasone*")==1   //  

replace ics_24h_level = 4 if dose_24h>400 & dose_24h!=. & strmatch(prescribedbnfname,"*qvar*")==1 & strmatch(substancename,"*beclometasone*")==1   // 


*soprobec  ;; mainly given to the under 12s
replace ics_24h_level = 2 if dose_24h<=400 & strmatch(prescribedbnfname,"*soprobec*")==1 & strmatch(substancename,"*beclometasone*")==1   // 

replace ics_24h_level = 3 if dose_24h>400 & dose_24h<=800 & strmatch(prescribedbnfname,"*soprobec*")==1 & strmatch(substancename,"*beclometasone*")==1   // no changes

replace ics_24h_level = 4 if dose_24h>800 & dose_24h!=. & strmatch(prescribedbnfname,"*soprobec*")==1 & strmatch(substancename,"*beclometasone*")==1    // no changes



* ciclesonide only inhaler 
replace ics_24h_level = 2 if dose_24h<=160 & (strmatch(prescribedbnfname,"*alvesco*")==1 | strmatch(substancename,"*ciclesonide*")==1)   //

replace ics_24h_level = 3 if dose_24h>160 & dose_24h<=320 & (strmatch(prescribedbnfname,"*alvesco*")==1 | strmatch(substancename,"*ciclesonide*")==1)    //

replace ics_24h_level = 4 if dose_24h>320 & dose_24h!=. & (strmatch(prescribedbnfname,"*alvesco*")==1 | strmatch(substancename,"*ciclesonide*")==1)   //0


* fluticasone  only inhaler  //doses are same as the accualher powder inhaler 
replace ics_24h_level = 2 if dose_24h<=200 & strmatch(prescribedbnfname,"*flixotide*evohaler*")==1 & strmatch(substancename,"*fluticasone*")==1    //

replace ics_24h_level = 3 if dose_24h>200 & dose_24h<=500 & strmatch(prescribedbnfname,"*flixotide*evohaler*")==1 & strmatch(substancename,"*fluticasone*")==1    //

replace ics_24h_level = 4 if dose_24h>500 & dose_24h!=. & strmatch(prescribedbnfname,"*flixotide*evohaler*")==1 & strmatch(substancename,"*fluticasone*")==1    //


**  non branded inhalers: 
*  single substance  inhalers : bec only
replace ics_24h_level = 2 if dose_24h<=400 & strmatch(prescribedbnfname,"*beclometasone*")==1   & ics_24h_level==.  // 

replace ics_24h_level = 3 if dose_24h>400 & dose_24h<=800 & strmatch(prescribedbnfname,"*beclometasone*")==1   & ics_24h_level==.  // 

replace ics_24h_level = 4 if dose_24h>800 & dose_24h!=. & strmatch(prescribedbnfname,"*beclometasone*")==1   & ics_24h_level==.  // 


** NOW POWDER INHALERS
** beclometasone only inhalers; no high dose specified

* asmabec ; given at higher dose added a high category)
replace ics_24h_level = 2 if dose_24h<=400 & strmatch(prescribedbnfname,"*asmabec*")==1 & strmatch(substancename,"*beclometasone*")==1 //

replace ics_24h_level = 3 if dose_24h>400 & dose_24h<=800 & strmatch(prescribedbnfname,"*asmabec*")==1 & strmatch(substancename,"*beclometasone*")==1 //


*easyhaler
replace ics_24h_level = 2 if dose_24h<=400 & strmatch(prescribedbnfname,"*easyhaler*")==1 & strmatch(substancename,"*beclometasone*")==1 & ics_24h_level==. // 

replace ics_24h_level = 3 if dose_24h>400 & dose_24h<=800 & strmatch(prescribedbnfname,"*easyhaler*")==1 & strmatch(substancename,"*beclometasone*")==1 & ics_24h_level==.  // 



** budesonide* only inhalers- 
replace ics_24h_level = 3 if dose_24h<=800 & strmatch(prescribedbnfname,"*budelin*novolizer*")==1 & strmatch(substancename,"*budesonide*")==1 //

replace ics_24h_level = 4 if dose_24h>800 & dose_24h!=. & strmatch(prescribedbnfname,"*budelin*novolizer*")==1 & strmatch(substancename,"*budesonide*")==1 //



replace ics_24h_level = 2 if dose_24h<=400 & strmatch(prescribedbnfname,"*pulmicort*turbohaler*")==1 & strmatch(substancename,"*budesonide*")==1 //

replace ics_24h_level = 3 if dose_24h>400 & dose_24h<=800 & strmatch(prescribedbnfname,"*pulmicort*turbohaler*")==1 & strmatch(substancename,"*budesonide*")==1    //

replace ics_24h_level = 4 if dose_24h>800 & dose_24h!=. & strmatch(prescribedbnfname,"*pulmicort*turbohaler*")==1 & strmatch(substancename,"*budesonide*")==1    //




**  this for generic bude only inhalers
replace ics_24h_level = 2 if dose_24h<=400 & strmatch(prescribedbnfname,"*easyhaler*")==1 & strmatch(substancename,"*budesonide*")==1 & ics_24h_level==.  // 

replace ics_24h_level = 3 if dose_24h>400 & dose_24h<=800 & strmatch(prescribedbnfname,"*easyhaler*")==1 & strmatch(substancename,"*budesonide*")==1 & ics_24h_level==.  //

replace ics_24h_level = 4 if dose_24h>800 & dose_24h!=. & strmatch(prescribedbnfname,"*easyhaler*")==1 & strmatch(substancename,"*budesonide*")==1 & ics_24h_level==.  //


** fluticasone only inhalers
replace ics_24h_level = 2 if dose_24h<=200 & strmatch(prescribedbnfname,"*flixotide*accuhaler*")==1 & strmatch(substancename,"*fluticasone*")==1 & ics_24h_level==. // 

replace ics_24h_level = 3 if dose_24h>200 & dose_24h<=500 & strmatch(prescribedbnfname,"*flixotide*accuhaler*")==1 & strmatch(substancename,"*fluticasone*")==1 & ics_24h_level==. //

replace ics_24h_level = 4 if dose_24h>500 & dose_24h!=. & strmatch(prescribedbnfname,"*flixotide*accuhaler*")==1 & strmatch(substancename,"*fluticasone*")==1 & ics_24h_level==. //



*** mometasone only inhalers
*  not many 
replace ics_24h_level = 2 if dose_24h<=400 & strmatch(prescribedbnfname,"*asmanex*twisthaler*")==1 & strmatch(substancename,"*mometasone*")==1 // 

replace ics_24h_level = 3 if dose_24h>400 & dose_24h<=800 & strmatch(prescribedbnfname,"*asmanex*twisthaler*")==1 & strmatch(substancename,"*mometasone*")==1 //


*******************************************************************************
** TACKLING THE COMBINATION inhalers

** beclometasone-based inhalers (with formoterol==t fostair)
** rely on formulation == combination to identify conbination inhalers 
replace ics_24h_level = 2 if dose_24h<=200 & strmatch(prescribedbnfname,"*fostair*")==1 & strmatch(substancename,"*beclometasone*")==1  //

replace ics_24h_level = 3 if dose_24h>200 & dose_24h<=400 & strmatch(prescribedbnfname,"*fostair*")==1 & strmatch(substancename,"*beclometasone*")==1   //

replace ics_24h_level = 4 if dose_24h>400 & dose_24h!=. & strmatch(prescribedbnfname,"*fostair*")==1 & strmatch(substancename,"*beclometasone*")==1   //

*Budesonide with formoterol
* duoresp
replace ics_24h_level = 2 if dose_24h<=320 & strmatch(prescribedbnfname,"*duoresp*")==1 & strmatch(substancename,"*budesonide*")==1  //

replace ics_24h_level = 3 if dose_24h>320 & dose_24h<=640 & strmatch(prescribedbnfname,"*duoresp*")==1 & strmatch(substancename,"*budesonide*")==1   //

replace ics_24h_level = 4 if dose_24h>640 & dose_24h!=. & strmatch(prescribedbnfname,"*duoresp*")==1 & strmatch(substancename,"*budesonide*")==1    // 

* symbicort 
replace ics_24h_level = 2 if dose_24h<=400 & strmatch(prescribedbnfname,"*symbicort*")==1 & strmatch(substancename,"*budesonide*")==1  //

replace ics_24h_level = 3 if dose_24h>400 & dose_24h<=800 & strmatch(prescribedbnfname,"*symbicort*")==1 & strmatch(substancename,"*budesonide*")==1   //

replace ics_24h_level = 4 if dose_24h>800 & dose_24h!=. & strmatch(prescribedbnfname,"*symbicort*")==1 & strmatch(substancename,"*budesonide*")==1    //

* fobumix
replace ics_24h_level = 2 if dose_24h<=320 & strmatch(prescribedbnfname,"*fobumix*")==1 & strmatch(substancename,"*budesonide*")==1  //

replace ics_24h_level = 3 if dose_24h>320 & dose_24h<=640 & strmatch(prescribedbnfname,"*fobumix*")==1 & strmatch(substancename,"*budesonide*")==1   //

replace ics_24h_level = 4 if dose_24h>640 & dose_24h!=. & strmatch(prescribedbnfname,"*fobumix*")==1 & strmatch(substancename,"*budesonide*")==1    //


*Fluticasone with formoterol
*note just used flutiform as the search term
replace ics_24h_level = 2 if dose_24h<=200 & strmatch(prescribedbnfname,"*flutiform*")==1 & strmatch(substancename,"*fluticasone*")==1   //

replace ics_24h_level = 3 if dose_24h>200 & dose_24h<=500 & strmatch(prescribedbnfname,"*flutiform*")==1 & strmatch(substancename,"*fluticasone*")==1   //

replace ics_24h_level = 4 if dose_24h>500 & dose_24h!=. & strmatch(prescribedbnfname,"*flutiform*")==1 & strmatch(substancename,"*fluticasone*")==1    //

*Fluticasone with salmeterol
** Aerivio Spiromax :
replace ics_24h_level = 4 if dose_24h>=1000 & dose_24h!=. & strmatch(prescribedbnfname,"*aerivio*")==1 & strmatch(substancename,"*fluticasone*")==1   //0

* note two types of airflusal inhalers:: 
** airflusal forspiro :only high dose listed; so make it equal to and over 500 
replace ics_24h_level = 4 if dose_24h>=1000 & dose_24h!=. & strmatch(prescribedbnfname,"*airflusal*forspiro")==1 & strmatch(substancename,"*fluticasone*")==1   //

** airflusal MDI: only medium and high dose listed
replace ics_24h_level = 3 if dose_24h<=500  & strmatch(prescribedbnfname,"*airflusal*")==1 & strmatch(substancename,"*fluticasone*")==1   //

replace ics_24h_level = 4 if dose_24h>500 & dose_24h!=. & strmatch(prescribedbnfname,"*airflusal*")==1 & strmatch(substancename,"*fluticasone*")==1   //

** Aloflute pMDI only medium and high dose listed; 
replace ics_24h_level = 3 if dose_24h<=500 & strmatch(prescribedbnfname,"*aloflute*")==1 & strmatch(substancename,"*fluticasone*")==1   //

replace ics_24h_level = 4 if dose_24h>500 & dose_24h!=. & strmatch(prescribedbnfname,"*aloflute*")==1 & strmatch(substancename,"*fluticasone*")==1   //

*Combisal (pMDI) 
replace ics_24h_level = 2 if dose_24h<=200 & strmatch(prescribedbnfname,"*combisal*")==1 & strmatch(substancename,"*fluticasone*")==1  // 

replace ics_24h_level = 3 if dose_24h>200 & dose_24h<=500 & strmatch(prescribedbnfname,"*combisal*")==1 & strmatch(substancename,"*fluticasone*")==1   // 

replace ics_24h_level = 4 if dose_24h>500 & dose_24h!=. & strmatch(prescribedbnfname,"*combisal*")==1 & strmatch(substancename,"*fluticasone*")==1    //

*fusacomb Easyhaler : medium and high dose only 
replace ics_24h_level = 3 if dose_24h<=500 & strmatch(prescribedbnfname,"*fusacomb*")==1 & strmatch(substancename,"*fluticasone*")==1   //

replace ics_24h_level = 4 if dose_24h>500 & dose_24h!=. & strmatch(prescribedbnfname,"*fusacomb*")==1 & strmatch(substancename,"*fluticasone*")==1   //

*sereflo pMDI : medium and high dose only 
replace ics_24h_level = 3 if dose_24h<=500 & strmatch(prescribedbnfname,"*sereflo*")==1 & strmatch(substancename,"*fluticasone*")==1   //

replace ics_24h_level = 4 if dose_24h>500 & dose_24h!=. & strmatch(prescribedbnfname,"*sereflo*")==1 & strmatch(substancename,"*fluticasone*")==1   //

* seretide accuhaler; 
replace ics_24h_level = 2 if dose_24h<=200 & strmatch(prescribedbnfname,"*seretide*accuhaler*")==1 & strmatch(substancename,"*fluticasone*")==1  //

replace ics_24h_level = 3 if dose_24h>200 & dose_24h<=500 & strmatch(prescribedbnfname,"*seretide*accuhaler*")==1 & strmatch(substancename,"*fluticasone*")==1   //

replace ics_24h_level = 4 if dose_24h>500 & dose_24h!=. & strmatch(prescribedbnfname,"*seretide*accuhaler*")==1 & strmatch(substancename,"*fluticasone*")==1    //

* evohaler
replace ics_24h_level = 2 if dose_24h<=200 & strmatch(prescribedbnfname,"*seretide*evohaler*")==1 & strmatch(substancename,"*fluticasone*")==1  //

replace ics_24h_level = 3 if dose_24h>200 & dose_24h<=500 & strmatch(prescribedbnfname,"*seretide*evohaler*")==1 & strmatch(substancename,"*fluticasone*")==1   //

replace ics_24h_level = 4 if dose_24h>500 & dose_24h!=. & strmatch(prescribedbnfname,"*seretide*evohaler*")==1 & strmatch(substancename,"*fluticasone*")==1    //


*sirdupla; medium and high doses only
replace ics_24h_level = 3 if dose_24h<=500 & strmatch(prescribedbnfname,"*sirdupla*")==1 & strmatch(substancename,"*fluticasone*")==1   //

replace ics_24h_level = 4 if dose_24h>500 & dose_24h!=. & strmatch(prescribedbnfname,"*sirdupla*")==1 & strmatch(substancename,"*fluticasone*")==1   //2


* stalpex: high dose only; 
replace ics_24h_level = 4 if dose_24h>=500 & dose_24h!=. & strmatch(prescribedbnfname,"*stalpex*")==1 & strmatch(substancename,"*fluticasone*")==1    //

* fluticasone furoate with vilanterol
* relvar Ellipta med and high dose only; all entries are relvar ellipta
replace ics_24h_level = 3 if dose_24h<=92 & strmatch(prescribedbnfname,"*relvar*")==1 & strmatch(substancename,"*fluticasone*")==1   //

replace ics_24h_level = 4 if dose_24h>92 & dose_24h!=. & strmatch(prescribedbnfname,"*relvar*")==1 & strmatch(substancename,"*fluticasone*")==1   // 

	
	*******************************************************
	**** Filling IN THE blanks for the 12 and over*********
	*******************************************************

** fluti/sal combo ; treat as  sertide or combisal (which have the same thresholds anyway) restrictions replacements to lines were theres no product name
replace ics_24h_level = 2 if dose_24h<=200 & strmatch(prescribedbnfname,"")==1 & strmatch(substancename,"*fluticasone*")==1  &  ics_24h_level==. // 

replace ics_24h_level = 3 if dose_24h>200 & dose_24h<=500 & strmatch(prescribedbnfname,"")==1 & strmatch(substancename,"*fluticasone*")==1   & ics_24h_level==. //

replace ics_24h_level = 4 if dose_24h>500 & dose_24h!=. & strmatch(prescribedbnfname,"")==1 & strmatch(substancename,"*fluticasone*")==1   & ics_24h_level==.  //

** bude=formo combo : base on symbicort as this is the most popular branded combo of this type
replace ics_24h_level = 2 if dose_24h<=400 & strmatch(prescribedbnfname,"")==1 & strmatch(substancename,"*budesonide*")==1  & ics_24h_level==. // 

replace ics_24h_level = 3 if dose_24h>400 & dose_24h<=800 & strmatch(prescribedbnfname,"")==1 & strmatch(substancename,"*budesonide*")==1   & ics_24h_level==. //

replace ics_24h_level = 4 if dose_24h>800 & dose_24h!=. & strmatch(prescribedbnfname,"")==1 & strmatch(substancename,"*budesonide*")==1   & ics_24h_level==. //

*** flutic only ; flioxotide comes as either  a pressutied or dry powder but thrsholds are the same so can use either but still specify inhaler type
replace ics_24h_level = 2 if dose_24h<=200 & strmatch(prescribedbnfname,"")==1 & strmatch(substancename,"*fluticasone*")==1   & ics_24h_level==. //

replace ics_24h_level = 3 if dose_24h>200 & dose_24h<=500 & strmatch(prescribedbnfname,"")==1 & strmatch(substancename,"*fluticasone*")==1   & ics_24h_level==. //

replace ics_24h_level = 4 if dose_24h>500 & dose_24h!=. & strmatch(prescribedbnfname,"")==1 & strmatch(substancename,"*fluticasone*")==1   & ics_24h_level==. //

replace ics_24h_level = 2 if dose_24h<=200 & strmatch(prescribedbnfname,"")==1 & strmatch(substancename,"*fluticasone*")==1 & ics_24h_level==. // 

replace ics_24h_level = 3 if dose_24h>200 & dose_24h<=500 & strmatch(prescribedbnfname,"")==1 & strmatch(substancename,"*fluticasone*")==1 & ics_24h_level==. //

replace ics_24h_level = 4 if dose_24h>500 & dose_24h!=. & strmatch(prescribedbnfname,"")==1 & strmatch(substancename,"*fluticasone*")==1 & ics_24h_level==. // 

** bude only inhaler; base on the non prop thresholds
replace ics_24h_level = 2 if dose_24h<=400 & strmatch(prescribedbnfname,"")==1 & strmatch(substancename,"*budesonide*")==1 & ics_24h_level==.  // 

replace ics_24h_level = 3 if dose_24h>400 & dose_24h<=800 & strmatch(prescribedbnfname,"")==1 & strmatch(substancename,"*budesonide*")==1 & ics_24h_level==.  // 

replace ics_24h_level = 4 if dose_24h>800 & dose_24h!=. & strmatch(prescribedbnfname,"")==1 & strmatch(substancename,"*budesonide*")==1 & ics_24h_level==. // 

** ciclesonide only; base on alvesco
replace ics_24h_level = 2 if dose_24h<=160 & strmatch(prescribedbnfname,"")==1 & strmatch(substancename,"*ciclesonide*")==1   & ics_24h_level==.  //

replace ics_24h_level = 3 if dose_24h>160 & dose_24h<=320 & strmatch(prescribedbnfname,"")==1 & strmatch(substancename,"*ciclesonide*")==1   & ics_24h_level==. //

replace ics_24h_level = 4 if dose_24h>320 & dose_24h!=. & strmatch(prescribedbnfname,"")==1 & strmatch(substancename,"*ciclesonide*")==1   & ics_24h_level==. //

* mometasone only base on Asmanex 
replace ics_24h_level = 2 if dose_24h<=400 & strmatch(prescribedbnfname,"")==1 & strmatch(substancename,"*mometasone*")==1 & ics_24h_level==. // 

replace ics_24h_level = 3 if dose_24h>400 & dose_24h<=800 & strmatch(prescribedbnfname,"")==1 & strmatch(substancename,"*mometasone*")==1 & ics_24h_level==. //

*** next target generics
**  fluti+sal; 
replace ics_24h_level = 2 if dose_24h<=200 & strmatch(prescribedbnfname,"*fluticasone*")==1 & strmatch(substancename,"*fluticasone*")==1  &  ics_24h_level==. // 

replace ics_24h_level = 3 if dose_24h>200 & dose_24h<=500 & strmatch(prescribedbnfname,"*fluticasone*")==1 & strmatch(substancename,"*fluticasone*")==1   & ics_24h_level==. // 

replace ics_24h_level = 4 if dose_24h>500 & dose_24h!=. & strmatch(prescribedbnfname,"*fluticasone*")==1 & strmatch(substancename,"*fluticasone*")==1   & ics_24h_level==.  // 

* next target fluticasone only non branded inhalers (quite a few again) flixotide accuhaler (DP)  or evohaler PI) but thresholds are the sme 200,500,1000
** note only put fluticasone in product name as not all rows include 
replace ics_24h_level = 2 if dose_24h<=200 & strmatch(prescribedbnfname,"*fluticasone*")==1 & strmatch(substancename,"*fluticasone*")==1   & ics_24h_level==. //

replace ics_24h_level = 3 if dose_24h>200 & dose_24h<=500 & strmatch(prescribedbnfname,"*fluticasone*")==1 & strmatch(substancename,"*fluticasone*")==1   & ics_24h_level==. //

replace ics_24h_level = 4 if dose_24h>500 & dose_24h!=. & strmatch(prescribedbnfname,"*fluticasone*")==1 & strmatch(substancename,"*fluticasone*")==1   & ics_24h_level==. //

replace ics_24h_level = 2 if dose_24h<=200 & strmatch(prescribedbnfname,"*fluticasone*")==1 & strmatch(substancename,"*fluticasone*")==1 & ics_24h_level==. // 

replace ics_24h_level = 3 if dose_24h>200 & dose_24h<=500 & strmatch(prescribedbnfname,"*fluticasone*")==1 & strmatch(substancename,"*fluticasone*")==1 & ics_24h_level==. //

replace ics_24h_level = 4 if dose_24h>500 & dose_24h!=. & strmatch(prescribedbnfname,"*fluticasone*")==1 & strmatch(substancename,"*fluticasone*")==1 & ics_24h_level==. // 

*bude plus formoterol combo: base on most popular as thresholds vary low is euher 320 or 400 so symbocort whihc has 400, 800
replace ics_24h_level = 2 if dose_24h<=400 & strmatch(prescribedbnfname,"*budesonide*")==1 & strmatch(substancename,"*budesonide*")==1  & ics_24h_level==. //  

replace ics_24h_level = 3 if dose_24h>400 & dose_24h<=800 & strmatch(prescribedbnfname,"*budesonide*")==1 & strmatch(substancename,"*budesonide*")==1   & ics_24h_level==. //

replace ics_24h_level = 4 if dose_24h>800 & dose_24h!=. & strmatch(prescribedbnfname,"*budesonide*")==1 & strmatch(substancename,"*budesonide*")==1   & ics_24h_level==. //	 

* budesonide only inhaler; base on non prop 400, 800; note dry powder only 
replace ics_24h_level = 2 if dose_24h<=400 & strmatch(prescribedbnfname,"*budesonide*")==1 & strmatch(substancename,"*budesonide*")==1 & ics_24h_level==.  // 

replace ics_24h_level = 3 if dose_24h>400 & dose_24h<=800 & strmatch(prescribedbnfname,"*budesonide*")==1 & strmatch(substancename,"*budesonide*")==1 & ics_24h_level==.  //

replace ics_24h_level = 4 if dose_24h>800 & dose_24h!=. & strmatch(prescribedbnfname,"*budesonide*")==1 & strmatch(substancename,"*budesonide*")==1 & ics_24h_level==. // 

replace ics_24h_level = 2 if dose_24h<=400 & strmatch(prescribedbnfname,"*budesonide*")==1 & strmatch(substancename,"*budesonide*")==1   & ics_24h_level==.  // 

replace ics_24h_level = 3 if dose_24h>400 & dose_24h<=800 & strmatch(prescribedbnfname,"*budesonide*")==1 & strmatch(substancename,"*budesonide*")==1   & ics_24h_level==.  // 

replace ics_24h_level = 4 if dose_24h>800 & dose_24h!=. & strmatch(prescribedbnfname,"*budesonide*")==1 & strmatch(substancename,"*budesonide*")==1   & ics_24h_level==. // 


** bec only generics; for  pressurized base on non prop thresholds 400, 800
* a11 powders are "high" dose; picked up later 
replace ics_24h_level = 2 if dose_24h<=400 & strmatch(prescribedbnfname,"*beclometasone*")==1 & strmatch(substancename,"*beclometasone*")==1   & ics_24h_level==.  // 

replace ics_24h_level = 3 if dose_24h>400 & dose_24h<=800  & strmatch(prescribedbnfname,"*beclometasone*")==1   & ics_24h_level==.  //  

replace ics_24h_level = 4 if dose_24h>800 & dose_24h!=. & strmatch(prescribedbnfname,"*beclometasone*")==1 & strmatch(substancename,"*beclometasone*")==1   & ics_24h_level==.  //  

** bec only generics; as dry powders base on non prop thresholds 400, 800 but no high dose; so miss those which dont mention easyhaler
replace ics_24h_level = 2 if dose_24h<=400 & strmatch(prescribedbnfname,"*beclometasone*")==1 & strmatch(substancename,"*beclometasone*")==1 & ics_24h_level==. // 

replace ics_24h_level = 3 if dose_24h>400 & dose_24h<=800 & strmatch(prescribedbnfname,"*beclometasone*")==1 & strmatch(substancename,"*beclometasone*")==1 & ics_24h_level==.  // 

** bec plus formeterol; base on fostair 200/400
replace ics_24h_level = 2 if dose_24h<=200 & strmatch(prescribedbnfname,"*beclometasone*")==1 & strmatch(substancename,"*beclometasone*")==1  & ics_24h_level==. // 

replace ics_24h_level = 3 if dose_24h>200 & dose_24h<=400 & strmatch(prescribedbnfname,"*beclometasone*")==1 & strmatch(substancename,"*beclometasone*")==1    & ics_24h_level==. // 

replace ics_24h_level = 4 if dose_24h>400 & dose_24h!=. & strmatch(prescribedbnfname,"*beclometasone*")==1 & strmatch(substancename,"*beclometasone*")==1   & ics_24h_level==. // 

** flutic plus former: base on flitiform 200/500
replace ics_24h_level = 2 if dose_24h<=200 & strmatch(prescribedbnfname,"*fluticasone*")==1 & strmatch(substancename,"*fluticasone*")==1  & ics_24h_level==. // 

replace ics_24h_level = 3 if dose_24h>200 & dose_24h<=500 & strmatch(prescribedbnfname,"*fluticasone*")==1 & strmatch(substancename,"*fluticasone*")==1   & ics_24h_level==. // 

replace ics_24h_level = 4 if dose_24h>500 & dose_24h!=. & strmatch(prescribedbnfname,"*fluticasone*")==1 & strmatch(substancename,"*fluticasone*")==1   & ics_24h_level==. // 

** ciclesonide only generic?  base on alvesco pressurized only
replace ics_24h_level = 2 if dose_24h<=160 & strmatch(prescribedbnfname,"*ciclesonide*")==1 & strmatch(substancename,"*ciclesonide*")==1   & ics_24h_level==.  //

replace ics_24h_level = 3 if dose_24h>160 & dose_24h<=320 & strmatch(prescribedbnfname,"*ciclesonide*")==1 & strmatch(substancename,"*ciclesonide*")==1   & ics_24h_level==. //

replace ics_24h_level = 4 if dose_24h>320 & dose_24h!=. & strmatch(prescribedbnfname,"*ciclesonide*")==1 & strmatch(substancename,"*ciclesonide*")==1  & ics_24h_level==. //

** picks up spare pulicort drop turbohaler and fomualtion from criteria
replace ics_24h_level = 2 if dose_24h<=400 & strmatch(prescribedbnfname,"*pulmicort*")==1 & strmatch(substancename,"*budesonide*")==1  & ics_24h_level==. //

replace ics_24h_level = 3 if dose_24h>400 & dose_24h<=800 & strmatch(prescribedbnfname,"*pulmicort*")==1 & strmatch(substancename,"*budesonide*")==1   & ics_24h_level==. // 

replace ics_24h_level = 4 if dose_24h>800 & dose_24h!=. & strmatch(prescribedbnfname,"*pulmicort*")==1 & strmatch(substancename,"*budesonide*")==1  & ics_24h_level==. //

**  fostair a combo bec plus form: 
replace ics_24h_level = 3 if dose_24h>200 & dose_24h<=400 & strmatch(prescribedbnfname,"*fostair*")==1 & strmatch(substancename,"*beclometasone*")==1   & ics_24h_level==. //

** missed some flixotides because we specified evo and accuhaler - no need as doses the same regardless of inhaler type
replace ics_24h_level = 2 if dose_24h<=200 & strmatch(prescribedbnfname,"*flixotide*")==1 & strmatch(substancename,"*fluticasone*")==1 & ics_24h_level==. // 

replace ics_24h_level = 3 if dose_24h>200 & dose_24h<=500 & strmatch(prescribedbnfname,"*flixotide*")==1 & strmatch(substancename,"*fluticasone*")==1  & ics_24h_level==. // 

replace ics_24h_level = 4 if dose_24h>500 & dose_24h!=. & strmatch(prescribedbnfname,"*flixotide*")==1 & strmatch(substancename,"*fluticasone*")==1   & ics_24h_level==. //

* missed  asmabecs bec only powder inhaler 200/400 no high dose
* doses prescibed high so add this category and will push into highest step 
replace ics_24h_level = 4 if dose_24h>800 & dose_24h!=. & strmatch(prescribedbnfname,"*asmabec*")==1 & strmatch(substancename,"*beclometasone*")==1 & ics_24h_level==. //

* missed  flutic plus vilanterol: base on relvar med and high only
replace ics_24h_level = 3 if dose_24h<=92 & strmatch(prescribedbnfname,"*fluticasone*")==1 & strmatch(substancename,"*fluticasone*")==1   & ics_24h_level==. //

replace ics_24h_level = 4 if dose_24h>92 & dose_24h!=. & strmatch(prescribedbnfname,"*fluticasone*")==1 & strmatch(substancename,"*fluticasone*")==1   & ics_24h_level==. // 

** some high dose bec only inhalers -  higher strength inhalers - assign to a cat 4 
replace ics_24h_level = 4 if dose_24h>800 & dose_24h!=. & strmatch(prescribedbnfname,"*beclometasone*")==1 & strmatch(substancename,"*beclometasone*")==1 & ics_24h_level==.  // 

** just becodisks left- assign as per the powder bec only asmabec, inhalers in same strength 100mg 
replace ics_24h_level = 2 if dose_24h<=400 & strmatch(prescribedbnfname,"*becodisks*")==1 & strmatch(substancename,"*beclometasone*")==1 &  ics_24h_level==. // 

replace ics_24h_level = 3 if dose_24h>400 & dose_24h<=800 & strmatch(prescribedbnfname,"*becodisks*")==1 & strmatch(substancename,"*beclometasone*")==1 &  ics_24h_level==. //


replace ics_24h_level = 2 if dose_24h<=400 & strmatch(substancename,"*beclometasone*")==1 & ics_24h_level==. // 

replace ics_24h_level = 3 if dose_24h>400 & dose_24h<=800 & strmatch(substancename,"*beclometasone*")==1 & ics_24h_level==.  // 


replace ics_24h_level = 2 if dose_24h<=400 & strmatch(substancename,"*mometasone*")==1 & ics_24h_level==. // 

replace ics_24h_level = 3 if dose_24h>400 & dose_24h<=800 & strmatch(substancename,"*mometasone*")==1 & ics_24h_level==. //


save "ccu035_02_ics_24h.dta", replace

use "ccu035_02_ics_24h.dta", clear



macro drop _all

clear

log close

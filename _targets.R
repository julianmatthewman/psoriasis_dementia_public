# Setup -------------------------------------------------------------------
# This _targets.R file defines the {targets} pipeline.
# Run tar_make() to run the pipeline, tar_make(target) to run up to a defined target, and tar_read(target) to view the results.
library(targets)
library(tarchetypes)
library(crew)

# Define external paths
source("paths/paths.R")

# Source all functions from the "R" folder
sapply(list.files("R", pattern=".R", full.names = TRUE), source, .GlobalEnv)

# Distributed Computing ---------------------------------------------------
# Enable the controller in tar_options_set to run targets in parallel
controller <- crew::crew_controller_local(
	name = "my_controller",
	workers = 6,
	launch_max = 1000
)
# # 
# tar_config_set(
# 	seconds_meta_append = 15,
# 	seconds_reporter = 0.5
# )

# Set target-specific options such as packages.
tar_option_set(
	controller = controller,
	storage = "worker", retrieval = "worker",
	memory = "transient", garbage_collection = TRUE,
	# error = "null",
	# workspace_on_error = TRUE, # Save a workspace file for a target that errors out.
	packages = c(
		"arrow", #For reading parquet files
		"cli", #For producing command line interface elements such as loading bars
		"survival", # For survival analysis including Cox regression
		"haven", # To read and write Stata .dta
		"openxlsx", # To read and write Excel .xlxs 
		"broom", # To clean regression output
		"lubridate", # To manage dates
		"tidyverse", # For data management
		"collapse", #For fast data management
		"gtsummary" # To produce summary tables
	)
) 



# List of target objects.
list(
	# Specifications -------------------------------------------------------------
	
	tar_target(exposure, "exposed"),
	tar_target(exclusion, c("outcome before indexdate", "consultation in year before indexdate")),
	
	# ┠ Models -----
	tar_target(
	  comorbidities,
	  c("chronic_liver_disease", 
	    "chronic_lung_disease", 
	    "cardiovascular_disease", 
	    "cerebrovascular_disease",
	    "diabetes",
	    "depression",
	    "cholesterol", 
	    "hypertension", 
	    "chronic_kidney_disease")
	),
	tar_target(# Specify models for regression
	  model, 
	  c(# Main models
	    "crude" = "",
	    "A" = paste(c("", "imd", "calendar_period"), collapse = " + "),
	    "B" = paste(c("", "imd", "calendar_period", comorbidities), collapse = " + "),
	    "C" = paste(c("", "imd", "calendar_period", comorbidities, "ethnicity_4_na"), collapse = " + "),
	    # Lifestyle covariates
	    "D" = paste(c("", "imd", "calendar_period", comorbidities, "smoking_status", "alcohol_abuse", "obese"), collapse = " + "),
	    "E" = paste(c("", "imd", "calendar_period", comorbidities, "smoking_status_na", "alcohol_abuse", "obese_na"), collapse = " + "),
	    # Other sensitivity analyses
	    "F" = paste(c(" * frailty_score", "imd", "calendar_period", comorbidities), collapse = " + "),
	    "G" = paste(c("", "imd", "calendar_period", comorbidities, "hearing_loss", "hiv", "rheumatoid_arthritis", "multiple_sclerosis", "inflammatory_bowel_disease"), collapse = " + "),
	    "H" = paste(c("", "imd", "calendar_period", comorbidities, "sleep_disorders"), collapse = " + "),
	    "I" = paste(c("", "imd", "calendar_period", comorbidities,  "high_dose_ocs"), collapse = " + "),
	    "J" = paste(c("", "imd", "calendar_period", comorbidities, "psoriatic_arthritis"), collapse = " + ")
	  )
	),
	
	
	# ┠ Study dates -----
	
	tar_target(study_start, as.Date("1997-04-01")),
	tar_target(study_end, as.Date("2021-03-31")),
	
	
	# Inputs ------------------------------------------------------------------
	
	# ┠ File paths -----------------------------------------------------------------
	
	# Codebrowsers
	tar_target(aurum_medical, haven::read_dta(paste0(path_codebrowsers, "CPRDAurumMedical.dta"))),
	tar_target(aurum_product, haven::read_dta(paste0(path_codebrowsers, "CPRDAurumProduct.dta"))),
	
	# Denominator, define and extract
	tar_target(denominator_file, if(dummy_define) path_denominator),
	tar_target(define_obs_files, if(dummy_define) dir(path_define, pattern = "Define_Inc1_Observation.*\\.parquet", full.names = TRUE)),
	tar_target(define_drug_files, if(dummy_define) dir(path_define, pattern = "Define_Inc1_Drug.*\\.parquet", full.names = TRUE)),
	tar_target(files, if(dummy_extract) dir(path_extract, pattern = "Extract_Observation.*\\.parquet", full.names = TRUE)),
	tar_target(drug_files, if(dummy_extract) dir(path_extract, pattern = "Extract_DrugIssue.*\\.parquet", full.names = TRUE)),
	
	#Lookups
	tar_target(numunit, read_tsv(path_numunit, col_types = cols(.default = "c"))),
	tar_target(common_dosages, read_tsv(path_common_dosages, col_types = cols(.default = "c"))),
	
	# Extract IDs
	tar_target(extract_ids, read_lines(path_extract_ids) |> strsplit(",") |> unlist()),
	
	# HES eligibility
	tar_target(hes_eligibility, if(dummy_define) read_dta(path_hes_eligibility)),

	
	# ┠ Codelists for Define---------------------------------------------------------
	#Codelists must be in the correct folder as .csv files and have the same name as specified here
	
	tar_target( # Codelists used to extract eventdata
	  codelists_for_define, 
	  tribble(~name, ~codevar, ~extract_from,
	          #medcodeids
	          "psoriasis", "medcodeid", "observation",
	          "phototherapy", "medcodeid",	"observation",
	          "cholesterol",	"medcodeid",	"observation",
	          "ethnicity", "medcodeid",	"observation",
	          "bmi", "medcodeid",	"observation",
	          "alcohol_abuse",	"medcodeid",	"observation",
	          "cigarette_smoking",	"medcodeid",	"observation",
	          "sleep_disorders",	"medcodeid",	"observation",
	          
	          "chronic_kidney_disease",	"medcodeid",	"observation",
	          "serum_creatinine", "medcodeid",	"observation",
	          
	          "anaemia","medcodeid",	"observation",
	          "haemoglobin", "medcodeid",	"observation",
	          
	          "atrial_fibrillation","medcodeid",	"observation",
	          "chads_vasc_score", "medcodeid",	"observation",
	          
	          "falls","medcodeid",	"observation",
	          "falls_number", "medcodeid",	"observation",
	          
	          "thyroid_disease","medcodeid",	"observation",
	          "tsh_level","medcodeid",	"observation",
	          
	          "osteoporosis","medcodeid",	"observation",
	          "hip_dxa_t_score", "medcodeid",	"observation",
	          
	          "weight_loss_anorexia","medcodeid",	"observation",
	          "malnutrition_score", "medcodeid",	"observation",
	          
	          # prodcodeids
	          "oral_glucocorticoids",	"prodcodeid",	"drugissue",
	          "topical_calcineurin_inhibitors",	"prodcodeid",	"drugissue",
	          "systemic_treatments_for_psoriasis",	"prodcodeid",	"drugissue",
	          "alcoholism_drugs",	"prodcodeid",	"drugissue",
	          "sleep_drugs",	"prodcodeid",	"drugissue"
	  ) |> 
	    mutate(path=paste0("codelists/Aurum/", codevar, "/", name, " aurum codelist.csv"),
	           full=map(path, ~read_csv(.x, col_types = cols(.default = "c"))),
	           codes=map2(path, codevar, ~read_csv(.x, , col_types = cols(.default = "c"))[[.y]]),
	           icd_path=paste0("codelists/HES/", name, " icd-10 codelist.csv"),
	           icd_full=map(icd_path, ~read_csv(.x, col_types = cols(.default = "c"))),
	           icd_codes=map2(icd_path, "code", ~read_csv(.x, col_types = cols(.default = "c"))[[.y]]))
	),
	tar_target(drug_codelists, codelists_for_define[which(codelists_for_define$name %in% c("oral_glucocorticoids", "systemic_treatments_for_psoriasis")),]),
	tar_target(cholesterol_codelist, codelists_for_define[which(codelists_for_define$name == "cholesterol"),]),
	tar_target(ethnicity_codelist, codelists_for_define[which(codelists_for_define$name == "ethnicity"),]),
	tar_target(serum_creatinine_codelist, codelists_for_define[which(codelists_for_define$name == "serum_creatinine"),]),
	tar_target(bmi_codelist, codelists_for_define[which(codelists_for_define$name == "bmi"),]),
	tar_target(alcohol_abuse_codelist, codelists_for_define[which(codelists_for_define$name == "alcohol_abuse"),]),
	tar_target(alcoholism_drugs_codelist, codelists_for_define[which(codelists_for_define$name == "alcoholism_drugs"),]),
	tar_target(sleep_disorders_codelist, codelists_for_define[which(codelists_for_define$name == "sleep_disorders"),]),
	tar_target(sleep_drugs_codelist, codelists_for_define[which(codelists_for_define$name == "sleep_drugs"),]),
	tar_target(cigarette_smoking_codelist, codelists_for_define[which(codelists_for_define$name == "cigarette_smoking"),]),
	tar_target(chronic_kidney_disease_codelist, codelists_for_define[which(codelists_for_define$name == "chronic_kidney_disease"),]),
	tar_target(anaemia_codelist, codelists_for_define[which(codelists_for_define$name == "anaemia"),]),
	tar_target(haemoglobin_codelist, codelists_for_define[which(codelists_for_define$name == "haemoglobin"),]),
	tar_target(atrial_fibrillation_codelist, codelists_for_define[which(codelists_for_define$name == "atrial_fibrillation"),]),
	tar_target(chads_vacs_score_codelist, codelists_for_define[which(codelists_for_define$name == "chads_vacs_score"),]),
	tar_target(falls_codelist, codelists_for_define[which(codelists_for_define$name == "falls"),]),
	tar_target(falls_number_codelist, codelists_for_define[which(codelists_for_define$name == "falls_number"),]),
	tar_target(thyroid_disease_codelist, codelists_for_define[which(codelists_for_define$name == "thyroid_disease"),]),
	tar_target(tsh_level_codelist, codelists_for_define[which(codelists_for_define$name == "tsh_level"),]),
	tar_target(osteoporosis_codelist, codelists_for_define[which(codelists_for_define$name == "osteoporosis"),]),
	tar_target(hip_dxa_t_score_codelist, codelists_for_define[which(codelists_for_define$name == "hip_dxa_t_score"),]),
	tar_target(weight_loss_anorexia_codelist, codelists_for_define[which(codelists_for_define$name == "weight_loss_anorexia"),]),
	tar_target(malnutrition_score_codelist, codelists_for_define[which(codelists_for_define$name == "malnutrition_score"),]),
	tar_target(ocs_codelist, codelists_for_define[which(codelists_for_define$name == "oral_glucocorticoids"),]),
	
	
	tar_target(drugs, drug_codelists$name),
	
	
	# ┠ Codelists ---------------------------------------------------------
	
	tar_target( # Codelists used to extract eventdata
	  codelists, 
	  tribble(~name, ~label, ~codevar, ~extract_from,
	          #outcomes
	          "dementia", "Dementia (all cause)", "medcodeid",	"observation_hes",
	          "dementia_specific", "Dementia (specific)", "medcodeid",	"observation_hes",
	          "dementia_alzheimers", "Alzheimer's dementia", "medcodeid",	"observation_hes",
	          "dementia_vascular", "Vascular dementia", "medcodeid",	"observation_hes",
	          "depression", "Depression", "medcodeid",	"observation_hes",
	          #covariates
	          "cardiovascular_disease", "Cardiovascular disease",	"medcodeid",	"observation_hes",
	          "chronic_liver_disease", "Chronic liver disease",	"medcodeid",	"observation_hes",
	          "chronic_lung_disease", "Chronic lung disease",	"medcodeid",	"observation_hes",
	          "copd", "COPD",	"medcodeid",	"observation_hes",
	          "dyslipidemia", "Dyslipidemia", "medcodeid",	"observation_hes",
	          "hiv", "HIV",	"medcodeid",	"observation_hes",
	          "inflammatory_bowel_disease", "Inflammatory bowel disease",	"medcodeid",	"observation_hes",
	          "multiple_sclerosis", "Multiple sclerosis",	"medcodeid",	"observation_hes",
	          "myocardial_infarction","Myocardial infarction","medcodeid",	"observation_hes",
	          "obesity", "Obesity", "medcodeid",	"observation_hes",
	          "peripheral_artery_disease", "Peripheral artery disease","medcodeid",	"observation_hes",
	          "stroke","Stroke","medcodeid",	"observation_hes",
	          "thromboembolic_diseases","Thromboembolic diseases","medcodeid",	"observation_hes",
	          "rheumatoid_arthritis", "Rheumatoid arthritis",	"medcodeid",	"observation_hes",
	          #frailty
	          "activity_limitation","Activity limitation","medcodeid",	"observation",
	          "arthritis","Arthritis","medcodeid",	"observation",
	          "cerebrovascular_disease", "Cerebrovascular disease",	"medcodeid",	"observation",
	          "diabetes", "Diabetes mellitus", "medcodeid", "observation",
	          "dizziness","Dizziness","medcodeid",	"observation",
	          "dyspnoea","Dyspnoea","medcodeid",	"observation",
	          "foot_problems","Foot problems","medcodeid",	"observation",
	          "fragility_fracture","Fragility fracture","medcodeid",	"observation",
	          "hearing_loss", "Hearing loss",	"medcodeid",	"observation",
	          "heart_failure","Heart failure","medcodeid",	"observation",
	          "heart_valve_disease","Heart valve disease","medcodeid",	"observation",
	          "housebound","Housebound","medcodeid",	"observation",
	          "hypertension", "Hypertension", "medcodeid", "observation",
	          "hypotension_syncope","Hypotension / syncope","medcodeid",	"observation",
	          "ischaemic_heart_disease","Ischaemic heart disease","medcodeid",	"observation",
	          "mobility_transfer_problems","Mobility and transfer problems","medcodeid",	"observation",
	          "parkinsonism_tremor","Parkinsonism and tremor","medcodeid",	"observation",
	          "peptic_ulcer","Peptic ulcer","medcodeid",	"observation",
	          "peripheral_vascular_disease","Peripheral vascular disease","medcodeid",	"observation",
	          "requirement_for_care","Requirement for care","medcodeid",	"observation",
	          "respiratory_disease","Respiratory disease","medcodeid",	"observation",
	          "skin_ulcer","Skin ulcer","medcodeid",	"observation",
	          "social_vulnerability","Social vulnerability","medcodeid",	"observation",
	          "urinary_incontinence","Urinary incontinence","medcodeid",	"observation",
	          "urinary_system_disease","Urinary system disease","medcodeid",	"observation",
	          "visual_impairment","Visual impairment","medcodeid",	"observation",
	          #frailty (with potential lab tests, counts or scores)
	          "anaemia", "Anaemia", "medcodeid",	"observation",
	          "atrial_fibrillation","Atrial fibrillation", "medcodeid",	"observation",
	          "falls", "Falls", "medcodeid",	"observation",
	          "thyroid_disease", "Thyroid disease", "medcodeid",	"observation",
	          "osteoporosis", "Osteoporosis", "medcodeid",	"observation",
	          "weight_loss_anorexia", "Weight loss and anorexia", "medcodeid",	"observation",
	          "psoriatic_arthritis", "Psoriatic arthritis", "medcodeid",	"observation"
	          ) |> 
	    mutate(path=paste0("codelists/Aurum/", codevar, "/", name, " aurum codelist.csv"),
	           full=map(path, ~read_csv(.x, col_types = cols(.default = "c"))),
	           codes=map2(path, codevar, ~read_csv(.x, col_types = cols(.default = "c"))[[.y]]),
	           sum_obs=map_vec(full, \(x) x |> pull(any_of(c("observations", "drugissues"))) |> as.numeric() |> sum(na.rm = TRUE)),
	           icd_path=paste0("codelists/HES/", name, " icd-10 codelist.csv"),
	           icd_full=map(icd_path, ~read_csv(.x, col_types = cols(.default = "c"))),
	           icd_codes=map2(icd_path, "code", ~read_csv(.x, col_types = cols(.default = "c"))[[.y]]))
	),
	tar_target(outcome, codelists$name),
	tar_target(n_outcomes, length(outcome)),
	

  # ┠ Codelist checks ---------------------------------------------------------
	
	# Check if every code in every medcode/prodcode codelist can be found in the CPRD Aurum Medical or Product browsers, respectively
	tar_target(check_if_in_browser, stopifnot(all(unlist(map2(c(codelists_for_define$codes, codelists$codes), 
																														c(codelists_for_define$codevar, codelists$codevar), 
																														\(x,y)
																														if_else(y=="medcodeid", 
																																		all(x[[1]] %in% aurum_medical$medcodeid), 
																																		all(x[[1]] %in% aurum_product$prodcodeid))))))),	
	
	# ┠ Dummy data --------------------------------------------------------------
	
	tar_target(dummy_define, make_dummy_define_aurum(codelists_for_define)),
	tar_target(dummy_extract, make_dummy_extract_aurum(codelists, codelists_for_define, bind_rows(cohort_matched), outcome)),
	tar_target(dummy_outcome,
						 tibble(
						 	patid=sample(cohort_defined$patid, nrow(cohort_defined)/2),
						 	obsdate=as_date(sample(study_start:study_end, nrow(cohort_defined)/2, replace = TRUE)),
						 	medcodeid=1),
						 pattern = slice(cohort_defined, 1)),
	
	
	# Data management ---------------------------------------------------------
	
	# ┠ Read denominator and define------------------------------------------------
	
	tar_target(denominator, open_dataset(denominator_file) |> filter(patid %in% extract_ids) |> collect()),
	tar_target(yobs, denominator |> collapse::fselect(patid, yob)),
	
	tar_target(exposed_obs_defined, open_dataset(define_obs_files) |> 
	             filter(patid %in% extract_ids) |> 
	             select(patid, pracid, obsdate, medcodeid) |> collect() |> 
	             mutate(obsdate=as.Date(obsdate, "%d/%m/%Y")),
	           format = "parquet"),
	
	tar_target(exposed_drug_defined, open_dataset(define_drug_files) |> 
	             filter(patid %in% extract_ids) |> 
	             select(patid, prodcodeid, issuedate, dosageid, quantity, quantunitid, duration) |> collect() |> 
	             mutate(issuedate=as.Date(issuedate, "%d/%m/%Y")),
	           format = "parquet"),
	
	# ┠ Read linked data -----------------------------------------------
	tar_target(hes_diagnosis, if(dummy_extract) read_parquet(paste0(path_linked_data, "hes_diagnosis_epi_23_002911_DM.parquet"))),
	tar_target(ons_death, if(dummy_define) read_tsv_arrow(paste0(path_linked_data, "death_patient_23_002911_DM.txt"), schema = schema(patid=string(), dod=string()), skip = 1)),
	tar_target(imd_patient, if(dummy_define) read_tsv_arrow(paste0(path_linked_data, "patient_2019_imd_23_002911.txt"), schema = schema(patid=string(), pracid=int64(), e2019_imd_5=int64()), skip = 1)),
	tar_target(imd_practice, if(dummy_define) read_tsv_arrow(paste0(path_linked_data, "practice_imd_23_002911.txt"))),
	
	tar_target(psoriasis_hes_codes, unlist(codelists_for_define$icd_codes[codelists_for_define$name=="psoriasis"])),
	tar_target(
	  hes_diagnosis_psoriasis,
	  open_dataset(paste0(path_linked_data, "hes_diagnosis_epi_23_002911_DM.parquet")) |> 
	    filter(ICD %in% psoriasis_hes_codes) |> 
	    select(patid, obsdate=epistart, medcodeid=ICD) |> 
	    collect() |> 
	    mutate(obsdate=as.Date(obsdate, "%d/%m/%Y"))
	),

	
	
	# ┠ Define Exposed ------------------------------------------------
	
	tar_target(
	  cohort_psoriasis_all,
	  define_cohort_psoriasis(codelists_for_define,
	                          exposed_obs_defined,
	                          study_start,
	                          study_end),
	  format = "parquet"
	),
	
	
	tar_target(
	  cohort_defined,
	  yobs |> 
	    left_join(cohort_psoriasis_all, by="patid") |> 
	    collapse::ftransform(exposed_date=pmax(exposed_date, as.Date(paste0(yob+40, "-06-01")))) |> 
	    collapse::fsubset(!is.na(exposed_date), -yob) |> 
	    collapse::fsubset(patid %in% hes_eligible_patids),
	  format = "parquet"
	),
	
	

	
	# ┠ Create Cohort ------------------------------------------------------------
	
	# Create an initial cohort of people that are eligible for matching
	tar_target(
	  cohort_eligible_main,
	  create_cohort_eligible(denominator, cohort_defined, study_start, study_end),
	  format = "parquet",
	  deployment = "main"
	),
	tar_target(
	  cohort_eligible_pre_covid,
	  create_cohort_eligible(denominator, cohort_defined, study_start, as.Date("2020-03-01")),
	  format = "parquet",
	  deployment = "main"
	),
	tar_target(
	  cohort_eligible_post_2004,
	  create_cohort_eligible(denominator, cohort_defined, as.Date("2004-01-01"), study_end),
	  format = "parquet",
	  deployment = "main"
	),
	tar_target(
	  cohort_eligible_post_2006,
	  create_cohort_eligible(denominator, cohort_defined, as.Date("2006-01-01"), study_end),
	  format = "parquet",
	  deployment = "main"
	),
	tar_target(
	  cohort_eligible_incident,
	  create_cohort_eligible_incident(denominator, exposed_obs_defined, study_start, study_end, codelists_for_define),
	  format = "parquet",
	  deployment = "main"
	),
	tar_target(
	  cohort_eligible_65,
	  create_cohort_eligible_65(denominator, cohort_defined, study_start, study_end),
	  format = "parquet",
	  deployment = "main"
	),
	tar_target(
	  cohort_eligible,
	  list(cohort_eligible_main,
	       cohort_eligible_main,
	       cohort_eligible_pre_covid,
	       cohort_eligible_post_2004,
	       cohort_eligible_post_2006,
	       cohort_eligible_incident,
	       cohort_eligible_65),
	  iteration = "list"
	),
	
	# Create a matchable cohort
	# # Main
	tar_target(
	  cohort_matchable_main,
	  create_cohort_matchable(cohort_eligible_main),
	  format = "parquet"
	),
	tar_target(
	  cohort_matchable_grouped,
	  split(cohort_matchable_main, cohort_matchable_main$pracid),
	),
	tar_target(
	  cohort_matched_main,
	  create_cohort_matched(cohort_matchable_grouped),
	  format = "parquet",
	  deployment = "main"
	),
	
	# # Severe
	tar_target(
	  cohort_matchable_severe,
	  cohort_matchable_main |> join(psoriasis_severity, on="patid", how="left", multiple = TRUE),
	  format = "parquet"
	),
	tar_target(
	  cohort_matchable_severe_grouped,
	  split(cohort_matchable_severe, cohort_matchable_severe$pracid),
	),
	tar_target(
	  cohort_matched_severe,
	  create_cohort_matched_severe(cohort_matchable_severe_grouped),
	  format = "parquet",
	  deployment = "main"
	),
	
	# # Pre COVID
	tar_target(
	  cohort_matchable_pre_covid,
	  create_cohort_matchable(cohort_eligible_pre_covid),
	  format = "parquet"
	),
	tar_target(
	  cohort_matchable_pre_covid_grouped,
	  split(cohort_matchable_pre_covid, cohort_matchable_pre_covid$pracid),
	),
	tar_target(
	  cohort_matched_pre_covid,
	  create_cohort_matched(cohort_matchable_pre_covid_grouped),
	  format = "parquet",
	  deployment = "main"
	),
	
	# # Post 2004
	tar_target(
	  cohort_matchable_post_2004,
	  create_cohort_matchable(cohort_eligible_post_2004),
	  format = "parquet"
	),
	tar_target(
	  cohort_matchable_post_2004_grouped,
	  split(cohort_matchable_post_2004, cohort_matchable_post_2004$pracid),
	),
	tar_target(
	  cohort_matched_post_2004,
	  create_cohort_matched(cohort_matchable_post_2004_grouped),
	  format = "parquet",
	  deployment = "main"
	),
	
	# # Post 2006
	tar_target(
	  cohort_matchable_post_2006,
	  create_cohort_matchable(cohort_eligible_post_2006),
	  format = "parquet"
	),
	tar_target(
	  cohort_matchable_post_2006_grouped,
	  split(cohort_matchable_post_2006, cohort_matchable_post_2006$pracid),
	),
	tar_target(
	  cohort_matched_post_2006,
	  create_cohort_matched(cohort_matchable_post_2006_grouped),
	  format = "parquet",
	  deployment = "main"
	),
	
	# # Incident
	tar_target(
	  cohort_matchable_incident,
	  create_cohort_matchable(cohort_eligible_incident),
	  format = "parquet"
	),
	tar_target(
	  cohort_matchable_incident_grouped,
	  split(cohort_matchable_incident, cohort_matchable_incident$pracid),
	),
	tar_target(
	  cohort_matched_incident,
	  create_cohort_matched(cohort_matchable_incident_grouped),
	  format = "parquet",
	  deployment = "main"
	),
	
	# # 65+
	tar_target(
	  cohort_matchable_65,
	  create_cohort_matchable(cohort_eligible_65),
	  format = "parquet"
	),
	tar_target(
	  cohort_matchable_65_grouped,
	  split(cohort_matchable_65, cohort_matchable_65$pracid),
	),
	tar_target(
	  cohort_matched_65,
	  create_cohort_matched(cohort_matchable_65_grouped),
	  format = "parquet",
	  deployment = "main"
	),

	# All cohorts
	tar_target(
	  cohort_matched,
	  list(cohort_matched_main,
	       cohort_matched_severe,
	       cohort_matched_pre_covid,
	       cohort_matched_post_2004,
	       cohort_matched_post_2006,
	       cohort_matched_incident,
	       cohort_matched_65),
	  iteration = "list"
	),
	tar_target(cohort_labels,
	           c("main", 
	             "severe",
	             "pre_covid",
	             "post_2004",
	             "post_2006",
	             "incident",
	             "65+")),

	
	# ┠ Extract eventdata -------------------------------------------------------
	
	tar_target( # Get eventdata for every codelist
		eventdata,
		open_dataset(files) |> 
			select(patid, obsdate, medcodeid) |> 
			filter(medcodeid %in% unlist(codelists$codes)) |> 
		  mutate(obsdate=as.Date(obsdate, "%d/%m/%Y")) |> 
		  filter(obsdate>=study_start & obsdate<=study_end) |> 
			arrange(obsdate) |> 
			collect(),
		pattern = map(codelists),
		iteration = "list",
		format = "parquet"
	),
	
	tar_target( # Get eventdata for all drug codelists
		drug_eventdata,
		exposed_drug_defined |> 
			fsubset(patid %in% patids_from_all_cohorts) |> 
			fsubset(prodcodeid %in% unlist(drug_codelists$codes)) |> 
			roworder(issuedate) |> 
			fsubset(issuedate>=study_start & issuedate<=study_end),
		pattern = map(drug_codelists),
		iteration = "list",
		format = "parquet"
	),

	
	tar_target(
	  hes_eventdata,
	  hes_diagnosis |> 
	    fselect(patid, epistart, ICD) |> 
	    fsubset(ICD %in% unlist(codelists$icd_codes)) |> 
	    roworder(epistart) |> 
	    ftransform(epistart=as.Date(epistart, "%d/%m/%Y")) |> 
	    fsubset(epistart>=study_start & epistart<=study_end),
	  pattern = map(codelists),
	  iteration = "list",
	  format = "parquet"
	),
	
	# ┠ Create analysis cohort --------------------------------------------------
	
	tar_target(
	  pre_index_vars,
	  create_pre_index_vars(cohort_matched, eventdata, hes_eventdata, codelists),
	  pattern = cohort_matched,
	  iteration = "list",
	  format = "parquet",
	  deployment = "main"
	),
	tar_target(
	  drug_vars,
	  create_pre_index_drug_vars(cohort_matched, drug_eventdata, drug_codelists),
	  pattern = cohort_matched,
	  iteration = "list",
	  format = "parquet",
	),
	tar_target(
	  cohort_wide,
	  bind_cols(cohort_matched, pre_index_vars, drug_vars, ethnicity_vars, imd_vars, ckd_pre_index_var, alc_pre_index_var, cholesterol_pre_index_var, smok_var, bmi_var, sleep_pre_index_var, polypharmacy, cons_in_year_pre_index, high_dose_ocs_vars) |> 
	    alg_frailty(),
	  pattern = map(cohort_matched, pre_index_vars, drug_vars, ethnicity_vars, imd_vars, ckd_pre_index_var, alc_pre_index_var, cholesterol_pre_index_var, smok_var, bmi_var, sleep_pre_index_var, polypharmacy, cons_in_year_pre_index, high_dose_ocs_vars),
	  iteration = "list",
	  format = "parquet"
	),
	

	# Variables-------------------------------------------------------
	
	#┠ Polypharmacy--------------------------------
	tar_target(
	  polypharmacy,
	  alg_polypharmacy(cohort_matched, drug_files, aurum_product),
	  pattern = cohort_matched,
	  iteration = "list",
	  format = "parquet"
	),
	
	#Consultation in the year before cohort entry
	tar_target(
	  cons_in_year_pre_index,
	  alg_cons_in_year_pre_index(cohort_matched, files, aurum_medical),
	  pattern = cohort_matched,
	  iteration = "list",
	  format = "parquet"
	),
	
	# Alcohol abuse
	tar_target(alc_eventdata, alg_alcoholism(bind_rows(alcohol_abuse_codelist, alcoholism_drugs_codelist), files, drug_files, study_start, study_end)),
	tar_target(
	  alc_hes_eventdata, 
	  hes_diagnosis |> 
	    select(patid, epistart, ICD) |> 
	    filter(ICD %in% unlist(codelists_for_define$icd_codes[codelists_for_define$name=="alcohol_abuse"])) |> 
	    arrange(epistart) |> 
	    ftransform(epistart=as.Date(epistart, "%d/%m/%Y")) |> 
	    fsubset(epistart>=study_start & epistart<=study_end)
	),
	tar_target(alc_pre_index_var, create_pre_index_vars(cohort_matched, list(alc_eventdata), list(alc_hes_eventdata), alcohol_abuse_codelist),
	           pattern = cohort_matched,
	           iteration = "list",
	           format = "parquet",
	           deployment = "main"),
	
	#┠ Sleep problems-----------------------------------
	tar_target(sleep_eventdata, alg_sleep(bind_rows(sleep_disorders_codelist, sleep_drugs_codelist), files, drug_files, study_start, study_end)),
	tar_target(
	  sleep_hes_eventdata, 
	  hes_diagnosis |> 
	    select(patid, epistart, ICD) |> 
	    filter(ICD %in% unlist(codelists_for_define$icd_codes[codelists_for_define$name=="sleep_disorders"])) |> 
	    arrange(epistart) |> 
	    ftransform(epistart=as.Date(epistart, "%d/%m/%Y")) |> 
	    fsubset(epistart>=study_start & epistart<=study_end)
	),
	tar_target(sleep_pre_index_var, create_pre_index_vars(cohort_matched, list(sleep_eventdata), list(sleep_hes_eventdata), sleep_disorders_codelist),
	           pattern = cohort_matched,
	           iteration = "list",
	           format = "parquet",
	           deployment = "main"),
	
	
	#┠ Smoking status---------------------------
	tar_target(smok_medcodes, unlist(cigarette_smoking_codelist$codes)),
	tar_target(
	  smok_eventdata, 
	  open_dataset(files) |> 
	    select(patid, obsdate, medcodeid) |> 
	    filter(medcodeid %in% smok_medcodes) |> 
	    mutate(obsdate=as.Date(obsdate, "%d/%m/%Y")) |> 
	    filter(obsdate>=study_start & obsdate<=study_end) |> 
	    arrange(obsdate) |> 
	    collect() |> 
	    right_join(cigarette_smoking_codelist$full[[1]][c("medcodeid", "smokstatus")]),
	  format="parquet"
	),
	tar_target(
	  smok_var,
	  alg_smoking(cohort_matched, smok_eventdata),
	  pattern = cohort_matched,
	  iteration = "list",
	  format = "parquet"
	),

	#┠ BMI-----------------------------------
	tar_target(bmi_medcodes, unlist(bmi_codelist$codes)),
	tar_target(
	  bmi_eventdata, 
	  open_dataset(files) |> 
	    select(patid, obsdate, medcodeid, numunitid, value) |> 
	    filter(medcodeid %in% bmi_medcodes) |> 
	    mutate(obsdate=as.Date(obsdate, "%d/%m/%Y")) |> 
	    filter(obsdate>=study_start & obsdate<=study_end) |> 
	    arrange(obsdate) |> 
	    collect() |> 
	    right_join(bmi_codelist$full[[1]]) |> 
	    left_join(numunit, by="numunitid"),
	  format="parquet"
	),
	tar_target(
	  bmi_var,
	  alg_bmi(cohort_matched, bmi_eventdata),
	  pattern = cohort_matched,
	  iteration = "list",
	  format = "parquet"
	),
	
	#┠ Cholesterol--------------------------------------
	tar_target(cholesterol_medcodes, unlist(cholesterol_codelist$codes)),
	tar_target(
	  cholesterol_eventdata, 
	  open_dataset(files) |> 
	    select(patid, obsdate, medcodeid, numunitid, value) |> 
	    filter(medcodeid %in% cholesterol_medcodes) |> 
	    mutate(obsdate=as.Date(obsdate, "%d/%m/%Y")) |> 
	    filter(obsdate>=study_start & obsdate<=study_end) |> 
	    arrange(obsdate) |> 
	    collect() |> 
	    left_join(numunit, by="numunitid") |> 
	    left_join(cholesterol_codelist$full[[1]], by="medcodeid") |> 
	    fsubset((Description == "mmol/L" & cholesterol_type=="total" & value > 5 & value < 20) |
	              (Description == "mmol/L" & cholesterol_type=="ldl" & value < 4 & value <20) |
	              (code_type=="high")) |> 
	    fselect(patid, obsdate, medcodeid),
	  format="parquet"
	),
	tar_target(
	  cholesterol_hes_eventdata, 
	  hes_diagnosis |> 
	    select(patid, epistart, ICD) |> 
	    filter(ICD %in% unlist(codelists_for_define$icd_codes[codelists_for_define$name=="cholesterol"])) |> 
	    arrange(epistart) |> 
	    ftransform(epistart=as.Date(epistart, "%d/%m/%Y")) |> 
	    fsubset(epistart>=study_start & epistart<=study_end)
	),
	tar_target(cholesterol_pre_index_var, create_pre_index_vars(cohort_matched, list(cholesterol_eventdata), list(cholesterol_hes_eventdata), cholesterol_codelist),
	           pattern = cohort_matched,
	           iteration = "list",
	           format = "parquet",
	           deployment = "main"),

	

	
	#┠ Chronic kidney disease--------------------------------------
	tar_target(ckd_eventdata, alg_ckd(chronic_kidney_disease_codelist, serum_creatinine_codelist, files, drug_files, study_start, study_end, numunit, denominator)),
	tar_target(
	  ckd_hes_eventdata, 
	  hes_diagnosis |> 
	    select(patid, epistart, ICD) |> 
	    filter(ICD %in% unlist(codelists_for_define$icd_codes[codelists_for_define$name=="chronic_kidney_disease"])) |> 
	    arrange(epistart) |> 
	    ftransform(epistart=as.Date(epistart, "%d/%m/%Y")) |> 
	    fsubset(epistart>=study_start & epistart<=study_end)
	),
	tar_target(ckd_pre_index_var, create_pre_index_vars(cohort_matched, list(ckd_eventdata), list(ckd_hes_eventdata), chronic_kidney_disease_codelist),
	           pattern = cohort_matched,
	           iteration = "list",
	           format = "parquet",
	           deployment = "main"),
	
	
	#┠ Ethnicity--------------------------------------
	tar_target(
	  ethnicity,
	  alg_ethnicity(ethnicity_codelist, files)
	),
	tar_target(
	  ethnicity_vars,
	  cohort_matched |> 
	    left_join(ethnicity, by="patid") |> 
	    select(ethnicity_5, ethnicity_16) |> 
	    mutate(ethnicity_5=as_factor(ethnicity_5), 
	           ethnicity_5_na=ethnicity_5,
	           ethnicity_5=fct_na_value_to_level(ethnicity_5,  level="5"),
	           ethnicity_16=as_factor(ethnicity_16),
	           ethnicity_16_na=ethnicity_16,
	           ethnicity_16=fct_na_value_to_level(ethnicity_16,  level="17"),
	           ethnicity_4_na=fct_collapse(ethnicity_5_na, white="0",south_asian="1", black="2", mixed_other=c("3", "4"))),
	  pattern = cohort_matched,
	  iteration = "list",
	  format = "parquet"
	),
	
	
	#┠ IMD------------------------------------
	tar_target(
	  imd,
	  imd_patient |> 
	    rename(imd=e2019_imd_5) |> 
	    left_join(imd_practice, by="pracid") |> 
	    mutate(imd=ifelse(is.na(imd), coalesce(e2019_imd_5, ni2017_imd_5, s2020_imd_5, w2019_imd_5), imd)) |> 
	    select(patid, imd)
	),
	tar_target(
	  imd_vars,
	  cohort_matched |> left_join(imd, by="patid") |> select(imd) |> mutate(imd=as_factor(imd)),
	  pattern = cohort_matched,
	  iteration = "list",
	  format = "parquet"
	),
	
	#┠ High dose OCS---------------------------------
	
	tar_target(
	  high_dose_ocs_eventdata,
	  alg_high_dose_oral_glucocorticoids(drug_eventdata, drug_codelists, common_dosages)
	),
	tar_target(
	  high_dose_ocs_vars, 
	  create_pre_index_drug_vars(cohort_matched, list(high_dose_ocs_eventdata), tibble(name="high_dose_ocs")),
	  pattern = cohort_matched,
	  iteration = "list",
	  format = "parquet",
	  deployment = "main"
	),
	
	# ┠ Psoriasis severity--------------------------------------------------
	
	tar_target(
	  psoriasis_severity,
	  alg_psoriasis_severity(exposed_obs_defined, exposed_drug_defined, codelists_for_define, hes_diagnosis_psoriasis)
	),
	

	# Analysis -------------------------------------------------------------------
	
	# ┠ Baseline & Flow ------------------------------------------------------------------
	
	tar_target(
		cohort_flow,
		analysis_cohort_flow(denominator, cohort_defined, cohort_labels, cohort_matched, cohort_eligible),
		pattern = map(cohort_labels, cohort_matched, cohort_eligible),
		deployment = "main"
	),
	tar_target(
		baseline_chars,
		analysis_baseline_chars(cohort_wide, denominator, cohort_labels),
		pattern = map(cohort_wide, cohort_labels),
		iteration = "vector"
	),
	tar_target(cohort_wide_ethn, cohort_wide[[1]] |> fsubset(!is.na(ethnicity_5_na))),
	tar_target(cohort_wide_ethn_missing, cohort_wide[[1]] |> fsubset(is.na(ethnicity_5_na))),
	tar_target(cohort_wide_smok, cohort_wide[[1]] |> fsubset(!is.na(smoking_status_na))),
	tar_target(cohort_wide_smok_missing, cohort_wide[[1]] |> fsubset(is.na(smoking_status_na))),
	tar_target(cohort_wide_obese, cohort_wide[[1]] |> fsubset(!is.na(obese_na))),
	tar_target(cohort_wide_obese_missing, cohort_wide[[1]] |> fsubset(is.na(obese_na))),
	tar_target(cohort_wide_smok_obese, cohort_wide[[1]] |> fsubset(!is.na(obese_na) & !is.na(smoking_status_na))),
	tar_target(cohort_wide_smok_obese_missing, cohort_wide[[1]] |> fsubset(is.na(obese_na) | is.na(smoking_status_na))),
	tar_target(cohort_wide_ethn_smok_obese, cohort_wide[[1]] |> fsubset(!is.na(obese_na) & !is.na(smoking_status_na) & !is.na(ethnicity_5_na))),
	tar_target(cohort_wide_ethn_smok_obese_missing, cohort_wide[[1]] |> fsubset(is.na(obese_na) | is.na(smoking_status_na) | is.na(ethnicity_5_na))),
	tar_target(cohort_wide_complete_case, list(cohort_wide_ethn, cohort_wide_smok, cohort_wide_obese, cohort_wide_smok_obese, cohort_wide_ethn_smok_obese,
	                                           cohort_wide_ethn_missing, cohort_wide_smok_missing, cohort_wide_obese_missing, cohort_wide_smok_obese_missing, cohort_wide_ethn_smok_obese_missing), iteration = "list"),
	tar_target(complete_case_labels, c("ethn", "smok", "obese", "smok_obese", "ethn_smok_obese",
	                                   "ethn_missing", "smok_missing", "obese_missing", "smok_obese_missing", "ethn_smok_obese_missing")),
	tar_target(
	  baseline_chars_complete_case,
	  analysis_baseline_chars(cohort_wide_complete_case, denominator, complete_case_labels),
	  pattern = map(cohort_wide_complete_case, complete_case_labels),
	  iteration = "vector"
	),
	
	# ┠ HRs: Main cohort-----------------------------------------------------------------
	
	tar_target(
	  cohort_post_exclusion,
	  create_cohort_post_exclusion(outcome, eventdata, hes_eventdata, cohort_wide, exclusion),
	  pattern = cross(cohort_wide, 
	                  exclusion,
	                  slice(map(outcome, eventdata, hes_eventdata), c(1:5))),
	  iteration = "list",
	  format = "parquet"
	),
	tar_target(
	  cohort_split,
	  create_cohort_split(cohort_post_exclusion),
	  pattern = map(cohort_post_exclusion),
	  iteration = "list",
	  format = "parquet"
	),
	tar_target(
	  results_rates,
	  analysis_rates(outcome, cohort_post_exclusion, exclusion, cohort_labels, model, exposure="exposed"),
	  pattern = cross(map(cohort_post_exclusion,
	                      cross(cohort_labels,
	                            exclusion,
	                            slice(outcome, c(1:5)))),
	                  model)	  
	),
	
	tar_target(
	  results_hrs,
	  analysis_hrs(outcome, cohort_split, cohort_labels, exclusion, exposure, model),
	  pattern = cross(map(cohort_split,
	                      cross(cohort_labels,
	                            exclusion,
	                            slice(outcome, c(1:5)))),
	                  model)	  
	),
	
	tar_target(
	  results,
	  results_hrs |> 
	    left_join(results_rates, by=c("cohort", "outcome", "model", "exclusion")) |> 
	    rename(term=term.x)
	),
	tar_target(
	  branches,
	  expand_grid(cohort_labels, exclusion, outcome[1:5], model=names(model)) |> 
	    rownames_to_column() |> 
	    filter(cohort_labels=="main" | 
	             exclusion=="outcome before indexdate") |> 
	    filter(model %in% c("crude", "A", "B", "C") | cohort_labels %in% c("main", "65+")) |>
	    pull(rowname) |> as.numeric()
	),
	tar_target(
	  interactions,
	  analysis_interactions(outcome, cohort_post_exclusion, cohort_labels, exclusion, exposure),
	  pattern = slice(
	    map(cohort_post_exclusion,
	        cross(cohort_labels,
	              exclusion,
	              slice(outcome, c(1:5)))), 
	    c(1:5, 51:55))
	),

	
	# ┠ HRs: Severity cohort -----------------------------------------------------

	tar_target(psoriatic_arthritis_eventdata, eventdata[[which(codelists$name=="psoriatic_arthritis")]]),
	
	tar_target(
	  cohort_severity,
	  create_cohort_severity(cohort_post_exclusion, psoriasis_severity, psoriatic_arthritis_eventdata),
	  pattern = map(slice(cohort_post_exclusion, 1:10)),
	  iteration = "list",
	  format = "parquet"
	),
	tar_target(
	  cohort_severity_split,
	  create_cohort_split_severity(cohort_severity),
	  pattern = map(cohort_severity),
	  iteration = "list",
	  format = "parquet"
	),
	
	tar_target(
	  results_rates_severity,
	  analysis_rates(outcome, cohort_severity, exclusion, cohort_labels, model, exposure="psoriasis_severity"),
	  pattern = cross(map(cohort_severity,
	                      cross(slice(cohort_labels, 1),
	                            slice(exclusion, 1:2),
	                            slice(outcome, c(1:5)))),
	                  slice(model, c(1:4,11)))
	),
	
	tar_target(
	  results_hrs_severity,
	  analysis_hrs(outcome, cohort_severity_split, cohort_labels, exclusion, exposure="psoriasis_severity", model),
	  pattern = cross(map(cohort_severity_split,
	                      cross(slice(cohort_labels, 1),
	                            slice(exclusion, 1:2),
	                            slice(outcome, c(1:5)))),
	                  slice(model, c(1:4,11)))
	),
	tar_target(
	  results_severity,
	  results_hrs_severity |> 
	    left_join(results_rates_severity, by=c("cohort", "outcome", "model", "exclusion", "term"))
	),
	
	# ┠ HRs: Psoriasis arthritis status -----------------------------------------------------
	
	tar_target(
	  results_rates_pa,
	  analysis_rates(outcome, cohort_severity, exclusion, cohort_labels, model, exposure="psoriatic_arthritis"),
	  pattern = cross(map(cohort_severity,
	                      cross(slice(cohort_labels, 1),
	                            slice(exclusion, 1:2),
	                            slice(outcome, c(1:5)))),
	                  slice(model, c(1:4)))
	),
	
	tar_target(
	  results_hrs_pa,
	  analysis_hrs(outcome, cohort_severity_split, cohort_labels, exclusion, exposure="psoriatic_arthritis", model),
	  pattern = cross(map(cohort_severity_split,
	                      cross(slice(cohort_labels, 1),
	                            slice(exclusion, 1:2),
	                            slice(outcome, c(1:5)))),
	                  slice(model, c(1:4)))
	),
	tar_target(
	  results_pa,
	  results_hrs_pa |> 
	    left_join(results_rates_pa, by=c("cohort", "outcome", "model", "exclusion", "term"))
	),
	
	
	# ┠ HRs: Time-updated cohort -----------------------------------------------------
	
	tar_target(
	  cohort_long,
	  create_cohort_long(cohort_wide, codelists, eventdata),
	  pattern = slice(map(cohort_wide),1),
	  iteration = "list",
	  format = "parquet"
	),
	tar_target(cohort_labels_long, cohort_labels[[1]]),
	
	tar_target(
	  cohort_post_exclusion_long,
	  create_cohort_post_exclusion_long(outcome, eventdata, hes_eventdata, cohort_long, cohort_labels_long),
	  pattern = cross(slice(map(outcome, eventdata, hes_eventdata), c(1:5)), map(cohort_long, cohort_labels_long)),
	  format = "parquet"
	),
	tar_target(
	  cohort_long_split,
	  create_cohort_split_severity(cohort_post_exclusion_long),
	  pattern = map(cohort_post_exclusion_long),
	  iteration = "list",
	  format = "parquet"
	),
	
	tar_target(
	  results_long,
	  analysis_long(outcome, cohort_long_split, cohort_labels_long, exposure, model, n_outcomes),
	  pattern = cross(map(cohort_long_split,
	                      cross(slice(cohort_labels, 1),
	                            slice(exclusion, 1),
	                            slice(outcome, c(1:5)))),
	                  slice(model, c(1:4, 11)))
	),
	
	
	# ┠ Checks ------------------------------------------------------------------
	
	tar_target( #Make lists of the most common codes for each eventdata
		common_codes,
		map2(eventdata, codelists$full, .progress = TRUE, \(x,y)
				 x |> 
				 	group_by(medcodeid) |> 
				 	tally() |> 
				 	left_join(y[c("medcodeid", "term")], by=c("medcodeid"="medcodeid")) |> 
				 	arrange(desc(n)) |> 
				 	filter(n>10)) |> 
			set_names(codelists$name) |> 
			bind_rows(.id = "column_label")
	),
	tar_target(
		common_codes_drugs,
		map(codelists_for_define[which(codelists_for_define$codevar=="prodcodeid"),]$full, \(x)
				x |> 
					left_join(exposed_drug_defined, by=c("prodcodeid" = "prodcodeid")) |> 
					group_by(termfromemis) |> 
					tally() |> 
					arrange(desc(n)) |> 
					filter(n>10))
	),
	tar_target(
		common_codes_aurum,
		map(codelists$full,
				\(x) x |>
					mutate(across(any_of(c("observations", "drugissues")), as.numeric)) |>
					arrange(desc(pick(any_of(c("observations", "drugissues"))))) |>
					mutate(across(any_of(c("observations", "drugissues")), \(x) if_else(is.na(x), 0, x))) |>
					mutate(running_perc=cumsum(pick(any_of(c("observations", "drugissues"))))/
								 	sum(pick(any_of(c("observations", "drugissues"))))) |>
					unnest_wider(running_perc, names_sep = "_") |>
					rename(any_of(c(running_perc = "running_perc_observations", running_perc = "running_perc_drugissues"))) |>
					filter(running_perc<=0.9 | row_number() < 6) |>
					select(any_of(c("running_perc", "observations", "Term", "drugissues", "Term from EMIS",
													"ProductName", "Formulation", "RouteOfAdministration", "DrugSubstanceName")))) |>
			set_names(str_sub(codelists$name, end=31))
	),

	
	# Write outputs -----------------------------------------------------------
	
	# ┠ for Define----------------------------------------------------
	
	#Codelists as single line for define
	tar_target(psoriasis_medcodes_for_define, codelists_for_define |> filter(name=="psoriasis") |> pull(codes) |> unlist()),
	tar_file(psoriasis_medcodes_for_define_file, write_single_line_and_return_path(psoriasis_medcodes_for_define)),
	tar_target(eczema_medcodes_for_define, codelists_for_define |> filter(name=="eczema") |> pull(codes) |> unlist()),
	tar_file(eczema_medcodes_for_define_file, write_single_line_and_return_path(eczema_medcodes_for_define)),
	tar_target(phototherapy_medcodes_for_define, codelists_for_define |> filter(name=="phototherapy") |> pull(codes) |> unlist()),
	tar_file(phototherapy_medcodes_for_define_file, write_single_line_and_return_path(phototherapy_medcodes_for_define)),
	tar_target(eczemaRx_prodcodes_for_define, codelists_for_define |> 
						 	filter(name %in% c("oral_glucocorticoids",
						 										 "emollients",
						 										 "topical_glucocorticoids",
						 										 "systemic_immunosupressants",
						 										 "topical_calcineurin_inhibitors")) |> 
						 	pull(codes) |> unlist() |> unique()),
	tar_file(eczemaRx_prodcodes_for_define_file, write_single_line_and_return_path(eczemaRx_prodcodes_for_define)),
	tar_target(psoriasisSysRx_prodcodes_for_define, codelists_for_define |> filter(name == "systemic_treatments_for_psoriasis") |> pull(codes) |> unlist() |> unique()),
	tar_file(psoriasisSysRx_prodcodes_for_define_file, write_single_line_and_return_path(psoriasisSysRx_prodcodes_for_define)),
	tar_target(all_medcodes_for_define, codelists_for_define[which(codelists_for_define$codevar=="medcodeid"),] |> pull(codes) |> unlist() |> unique()),
	tar_file(all_medcodes_for_define_file, write_single_line_chunked_and_return_path(all_medcodes_for_define, max=3000)),
	tar_target(all_prodcodes_for_define, codelists_for_define[which(codelists_for_define$codevar=="prodcodeid"),] |> pull(codes) |> unlist() |> unique()),
	tar_file(all_prodcodes_for_define_file, write_single_line_chunked_and_return_path(all_prodcodes_for_define, max=3000)),
	
	tar_target(medcodes_chunked_by_observations, write_medcodes_chunked_by_observations_and_return_path(codelists, maxfiles=20)),
	
	# ┠ for Extract-----------------------------------------------------
	tar_target(patids_from_all_cohorts, cohort_matched |> bind_rows() |> pull(patid) |> unique()),
	tar_target(patids_for_extract, 
						 cohort_matched[1],
						 pattern = cohort_matched, #Combine patids from all matched cohorts
						 iteration = "vector"),
	tar_target(patids_for_extract_unique, patids_for_extract |> filter(!duplicated(patid))),
	tar_file(patids_for_extract_file, write_delim_and_return_path(patids_for_extract_unique)),
	tar_target(patids_for_extract_chunked_files, write_delim_compressed_in_chunks_and_return_paths(patids_for_extract_unique, max=1000000)),
	
	# ┠ for Linkage----------------------------------------------------
	tar_target(hes_eligible_patids, hes_eligibility |> filter(hes_apc_e==1 & ons_death_e==1) |> pull(patid) |> as.character()),
	tar_target(hes_eligible_pats, patids_for_extract_unique |> 
						 	join(hes_eligibility, on="patid", multiple = FALSE) |> 
						 	fselect(patid, hes_apc_e, ons_death_e, lsoa_e) |> 
						 	fsubset(hes_apc_e==1) |> 
						 	fsubset(ons_death_e==1)),
	tar_file(hes_eligible_pats_file, write_delim_and_return_path(hes_eligible_pats)),
	tar_target(hes_eligible_pats_chunked_files, write_delim_chunked_and_return_path(hes_eligible_pats, 5000000)),
	
	
	# ┠ Results----------------------------------------------------
	
	#Tables as .csv
	tar_file(results_file, write_and_return_path(results)),
	tar_file(interactions_file, write_and_return_path(interactions)),
	tar_file(results_severity_file, write_and_return_path(results_severity)),
	tar_file(results_pa_file, write_and_return_path(results_pa)),
	tar_file(results_long_file, write_and_return_path(results_long)),
	tar_file(common_codes_file, write_and_return_path(common_codes)),
	tar_file(cohort_flow_file, write_and_return_path(cohort_flow)),
	tar_file(baseline_chars_file, write_and_return_path(baseline_chars)),
	tar_file(baseline_chars_complete_case_file, write_and_return_path(baseline_chars_complete_case)),
	
	#Vectors as text
	tar_file(exposure_file, write_lines_and_return_path(exposure)),
	tar_file(outcome_file, write_lines_and_return_path(outcome)),
	tar_file(model_file, write_lines_and_return_path(model)),
	tar_file(model_names_file, write_lines_and_return_path(names(model))),
	
	#Checks as Excel files
	tar_file(common_codes_aurum_file, write_xlsx_and_return_path(common_codes_aurum))
	
)


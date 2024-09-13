#' Extract the cohort
#' @description 
#' Get the first date someone has at least 1 diagnosis for psoriasis
define_cohort_psoriasis <- function(codelists_for_define,
                                    exposed_obs_defined,
                                    study_start,
                                    study_end) {
  
  
  
  #Get the first record for psoriasis for each person, if one exists
  psoriasis_codes <- codelists_for_define$codes[[which(codelists_for_define$name=="psoriasis")]]
  exposed_obs_defined |> 
    fselect(patid, obsdate, medcodeid) |> 
    fsubset(medcodeid %in% psoriasis_codes) |> 
    fsubset(obsdate>=as.Date(study_start)) |> 
    fsubset(obsdate<=as.Date(study_end)) |>
    arrange(obsdate) |> 
    filter(!duplicated(patid)) |> 
    select(patid, exposed_date=obsdate)
}


#' Create a cohort eligible for matching
#' @param cohort_defined A cohort with one row per person
#' @description
create_cohort_eligible <- function(denominator, cohort_defined, study_start, study_end) {
  denominator |> 
    fselect(-mob, -patienttypeid, -acceptable, -uts, -region, -emis_ddate) |> 
    join(cohort_defined, on="patid", how="left", multiple=FALSE) |> 
    ftransform(dob=as.Date(paste0(yob, "-06-01"))) |>
    
    #Date the person turn 40
    ftransform(fourty_date=as.Date(paste0(yob+40, "-06-01"))) |>
    
    #Date of Eligibility for matching: latest of: practice registration date plus 1 year, study start date (January 1, 1998). There is no up-to-standard date in CPRD Aurum (https://cprd.com/sites/default/files/2022-02/CPRD%20Aurum%20FAQs%20v2.2.pdf)
    ftransform(startdate=pmax(regstartdate+365, as.Date(study_start))) |> 
    
    #Not used: Adult date of eligibility for matching: latest of: startdate or adult date
    ftransform(startdate=pmax(startdate, fourty_date)) |>
    
    #Enddate: earliest of: end of registration, date of death, date of the last data collection from the practice, or the end of the study (31st January 2020).
    ftransform(enddate=pmin(regenddate, cprd_ddate, lcd, as.Date(study_end), na.rm = TRUE)) |> 
   
    #Indexdate: latest of being exposed and startdate
    ftransform(indexdate=pmax(exposed_date, startdate)) |> 
    
    #Set indexdates to NA that occur after the enddate
    ftransform(indexdate=if_else(indexdate>=enddate, NA_Date_, indexdate)) |> 
    
    #Keep only people with follow-up
    fsubset(enddate > startdate) |> 
    
    fselect(patid, yob, gender, pracid, indexdate, startdate, enddate, exposed_date)
}

#' Create a cohort eligible for matching with only incident cases of psoriasis
#' @param cohort_defined A cohort with one row per person
#' @description
create_cohort_eligible_incident <- function(denominator, exposed_obs_defined, study_start, study_end, codelists_for_define) {
  psoriasis_codes <- codelists_for_define$codes[[which(codelists_for_define$name=="psoriasis")]]
  cohort_defined_multiple <- exposed_obs_defined |> 
    fselect(patid, obsdate, medcodeid) |> 
    fsubset(medcodeid %in% psoriasis_codes) |> 
    fsubset(obsdate>=as.Date(study_start)) |> 
    fsubset(obsdate<=as.Date(study_end)) |>
    arrange(obsdate) |> 
    select(patid, exposed_date=obsdate)
  
  denominator |> 
    fselect(-mob, -patienttypeid, -acceptable, -uts, -region, -emis_ddate) |> 
    join(cohort_defined_multiple, on="patid", how="left", multiple = TRUE) |> 
    ftransform(dob=as.Date(paste0(yob, "-06-01"))) |>
    
    #Date the person turn 40
    ftransform(fourty_date=as.Date(paste0(yob+40, "-06-01"))) |>
    
    #Date of Eligibility for matching: latest of: practice registration date plus 1 year, study start date (January 1, 1998). There is no up-to-standard date in CPRD Aurum (https://cprd.com/sites/default/files/2022-02/CPRD%20Aurum%20FAQs%20v2.2.pdf)
    ftransform(startdate=pmax(regstartdate+365, as.Date(study_start))) |> 
    
    #Adult date of eligibility for matching: latest of: startdate or adult date
    ftransform(startdate=pmax(startdate, fourty_date)) |>
    
    #Enddate: earliest of: end of registration, date of death, date of the last data collection from the practice, or the end of the study (31st January 2020).
    ftransform(enddate=pmin(regenddate, cprd_ddate, lcd, as.Date(study_end), na.rm = TRUE)) |> 
    
    #Filter to only incident cases of psoriasis
    fsubset(exposed_date>startdate | is.na(exposed_date)) |> 
    roworder(exposed_date) |> 
    fsubset(!duplicated(patid)) |> 
    
    #Indexdate: latest of being exposed and startdate
    ftransform(indexdate=pmax(exposed_date, startdate)) |> 
    
    #Set indexdates to NA that occur after the enddate
    ftransform(indexdate=if_else(indexdate>=enddate, NA_Date_, indexdate)) |> 
    
    #Keep only people with follow-up
    fsubset(enddate > startdate) |> 
    
    fselect(patid, yob, gender, pracid, indexdate, startdate, enddate, exposed_date)
}

#' Create a cohort eligible for matching
#' @param cohort_defined A cohort with one row per person
#' @description
create_cohort_eligible_65 <- function(denominator, cohort_defined, study_start, study_end) {
  denominator |> 
    fselect(-mob, -patienttypeid, -acceptable, -uts, -region, -emis_ddate) |> 
    join(cohort_defined, on="patid", how="left", multiple = FALSE) |> 
    ftransform(dob=as.Date(paste0(yob, "-06-01"))) |>
    
    #Date the person turn 65
    ftransform(sixtyfive_date=as.Date(paste0(yob+65, "-06-01"))) |>
    
    #Date of Eligibility for matching: latest of: practice registration date plus 1 year, study start date (January 1, 1998). There is no up-to-standard date in CPRD Aurum (https://cprd.com/sites/default/files/2022-02/CPRD%20Aurum%20FAQs%20v2.2.pdf)
    ftransform(startdate=pmax(regstartdate+365, as.Date(study_start))) |> 
    
    #Not used: Adult date of eligibility for matching: latest of: startdate or adult date
    ftransform(startdate=pmax(startdate, sixtyfive_date)) |>
    
    #Enddate: earliest of: end of registration, date of death, date of the last data collection from the practice, or the end of the study (31st January 2020).
    ftransform(enddate=pmin(regenddate, cprd_ddate, lcd, as.Date(study_end), na.rm = TRUE)) |> 
    
    #Indexdate: latest of being exposed and startdate
    ftransform(indexdate=pmax(exposed_date, startdate)) |> 
    
    #Set indexdates to NA that occur after the enddate
    ftransform(indexdate=if_else(indexdate>=enddate, NA_Date_, indexdate)) |> 
    
    #Keep only people with follow-up
    fsubset(enddate > startdate) |> 
    
    fselect(patid, yob, gender, pracid, indexdate, startdate, enddate, exposed_date)
}


create_cohort_matchable <- function(cohort_eligible) {

	cohort_matchable <- cohort_eligible |> 
		ftransform(exposed=if_else(is.na(indexdate), 0, 1))
	
	# Get exposed patients to be available as controls prior to their date of first exposure
	pre_exposure_ppl <- cohort_matchable |> 
		fsubset(exposed==1) |> 
		mutate(enddate=indexdate-1, indexdate=NA_Date_, exposed=0) |> 
		fsubset(enddate > startdate)
	
	bind_rows(cohort_matchable, pre_exposure_ppl)
}

#' Create matched cohort using sequential trials matching
#' @description each daily trial includes all n eligible people who 
#' become exposed on that day (exposed=1) and
#' a sample of n eligible controls (exposed=0) who:
#' - had not been exposed on or before that day (still at risk of becoming exposed);
#' - still at risk of an outcome (not left the study); 
#' - had not already been selected as a control in a previous trial
#' @param grouped_cohort_to_match A data frame with one or two rows per participant with:
#'  1. $patid: The patient ID
#'  2. $startdate: the date people become eligible 
#'  3. $indexdate: the date eligible people got exposed 
#'  4. $enddate: the date people leave the study 
#'  @param dayspriorreg days prior registration required for controls
create_cohort_matched <- function(cohort_matchable_grouped, dayspriorreg=0) {
	
	#Map across every practice group
	library(future)
	plan(multisession, workers = 8)
	furrr::future_map_dfr(cohort_matchable_grouped, .progress = TRUE, \(x) {

	#Sort by indexdate, and then randomly
	cohort_matchable <- x |>
		ftransform(sortunique=runif(nrow(x))) |> 
		roworder(exposed, indexdate, sortunique) |> 
		fselect(-sortunique)

		exposed <- cohort_matchable |> fsubset(exposed==1) |> ftransform(setid=patid)
		unexposed <- cohort_matchable |> fsubset(exposed==0)
		
		matched <- exposed[0,] #Make empty dataframe with same columns to be filled
		
		#Loop through all people that ever get exposed (each one gets matched to people who are unexposed at the same time)
		for (i in 1:nrow(exposed)) {
			
			exposed_pat <- exposed[i,]
			matchday <- exposed_pat$indexdate
			
			#Drop people that can't be matched anymore (either because they have already been matched or they have passed the study end date)
			unexposed <- unexposed |> 
				fsubset(enddate > matchday & !(patid %in% matched$patid))
			
			#Perform matching
			new <- unexposed |> 
				fsubset(gender==exposed_pat$gender) |> 
				fsubset((startdate+dayspriorreg) <= matchday) |> 
				ftransform(age_difference=abs(yob-exposed_pat$yob)) |> 
				fsubset(age_difference<=2) |> 
				roworder(age_difference) |> # the closest matches are given priority
				slice(1:5) |> 
				ftransform(setid=exposed_pat$patid,
									 indexdate=as_date(matchday)) |>  #Set the indexdate for everyone to the day the exposed individual got exposed
				fselect(-age_difference)
			if (nrow(new)>0) matched <- bind_rows(matched, exposed_pat, new)
			# cli::cli_progress_update()
		}
		matched |> 
			roworder(setid, -exposed) |> 
			select(patid, exposed, indexdate, enddate, setid)})
}


create_cohort_matched_severe <- function(cohort_matchable_severe_grouped, dayspriorreg=0) {
  
  #Map across every practice group
  library(future)
  plan(multisession, workers = 8)
  furrr::future_map_dfr(cohort_matchable_severe_grouped, .progress = TRUE, \(x) {
    
    #Sort by indexdate, and then randomly
    cohort_matchable <- x |>
      ftransform(exposed=if_else(eventdate>=indexdate, 1, 0, missing=0)) |> #Here, only those with severe psoriasis recorded after the original indexdate are exposed
      ftransform(sortunique=runif(nrow(x))) |> 
      roworder(exposed, eventdate, sortunique) |> 
      fselect(-sortunique)
    
    exposed <- cohort_matchable |> fsubset(exposed==1) |> ftransform(setid=patid)
    unexposed <- cohort_matchable |> fsubset(exposed==0)
    
    matched <- exposed[0,] #Make empty dataframe with same columns to be filled
    
    #Loop through all people that ever get exposed (each one gets matched to people who are unexposed at the same time)
    for (i in 1:nrow(exposed)) {
      
      exposed_pat <- exposed[i,] |> 
        ftransform(indexdate=eventdate)
      
      #Here use the date where people have severe psoriasis
      matchday <- exposed_pat$eventdate
      
      #Drop people that can't be matched anymore (either because they have already been matched or they have passed the study end date)
      unexposed <- unexposed |> 
        fsubset((enddate > matchday | indexdate > matchday) & !(patid %in% matched$patid))
      
      #Perform matching
      new <- unexposed |> 
        fsubset(gender==exposed_pat$gender) |> 
        fsubset((startdate+dayspriorreg) <= matchday) |> 
        ftransform(age_difference=abs(yob-exposed_pat$yob)) |> 
        fsubset(age_difference<=2) |> 
        roworder(age_difference) |> # the closest matches are given priority
        slice(1:5) |> 
        ftransform(setid=exposed_pat$patid,
                   indexdate=as_date(matchday)) |>  #Set the indexdate for everyone to the day the exposed individual got exposed
        fselect(-age_difference)
      if (nrow(new)>0) matched <- bind_rows(matched, exposed_pat, new)
      # cli::cli_progress_update()
    }
    matched |> 
      roworder(setid, -exposed) |> 
      select(patid, exposed, indexdate, enddate, setid)
    })
}

#' Create pre-index event variables
#' @return A of dataframes each with one row per patient, with patient ID and a TRUE/FALSE variable if they had the event in the variable name prior to index date
create_pre_index_vars <- function(cohort_matched, eventdata, hes_eventdata, codelists) {
	pmap_dfc(list(eventdata, hes_eventdata, codelists$name), .progress=TRUE, \(eventdata, hes_eventdata, codelist_name) {
	  
	# Add HES data
	  eventdata_both <- hes_eventdata |> 
	    rename(obsdate=epistart, medcodeid=ICD) |> 
	    bind_rows(eventdata) |> 
	    arrange(obsdate)
	  
	# Get the first occurring event for each patient (that has an event)
	first_event <- eventdata_both |> 
		fselect(patid, obsdate) |> 
		fgroup_by(patid) |> 
		fmin()
	
	# Join the first events to the cohort of all patients
	temp <- cohort_matched[c("patid", "exposed", "indexdate")] |> 
		left_join(first_event, by=c("patid"))
	
	stopifnot(identical(temp[c("patid", "exposed")],cohort_matched[c("patid", "exposed")]))
	
	# Make a variable that is TRUE when the first event occurs before the index date
	temp[codelist_name] <- ifelse(temp$obsdate < temp$indexdate, TRUE, FALSE)
	temp[codelist_name][is.na(temp[codelist_name])] <- FALSE
	temp[codelist_name]
	})
	
}

#' Create pre-index event variables
#' @return A of dataframes each with one row per patient, with patient ID and a TRUE/FALSE variable if they had the event in the variable name prior to index date
create_pre_index_drug_vars <- function(cohort_matched, drug_eventdata, drug_codelists) {
	map2_dfc(drug_eventdata, drug_codelists$name, .progress=TRUE, \(drug_eventdata, codelist_name) {
		# Get the first occurring event for each patient (that has an event)
		first_event <- drug_eventdata |> 
			fselect(patid, issuedate) |> 
			fgroup_by(patid) |> 
			fmin()
		
		# Join the first events to the cohort of all patients
		temp <- cohort_matched[c("patid", "exposed", "indexdate")] |> 
			left_join(first_event, by=c("patid"))
		
		stopifnot(identical(temp[c("patid", "exposed")],cohort_matched[c("patid", "exposed")]))
		
		# Make a variable that is TRUE when the first event occur before the index date
		temp[codelist_name] <- ifelse(temp$issuedate < temp$indexdate, TRUE, FALSE)
		temp[codelist_name][is.na(temp[codelist_name])] <- FALSE
		temp[codelist_name]
	})
	
}


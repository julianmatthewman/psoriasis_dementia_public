create_cohort_post_exclusion <- function(outcome, eventdata, hes_eventdata, cohort_wide, exclusion) {
	#Get events
	events <- hes_eventdata |> 
	  select(patid, obsdate=epistart) |> 
	  bind_rows(eventdata[c("patid", "obsdate")]) |> 
	  roworder(obsdate) |> 
		collapse::funique.data.frame() #Only keep one row if there are multiple events for the same patient on the same day
	
	#Join events to the cohort
	temp <- cohort_wide |> 
		join(events, on="patid", how = "left", verbose=0, multiple=FALSE) #collapse::join() with the default argument multiple=FALSE only joins the first matching row of the events dataset to each row of the cohort, which is intended behaviour here
	
	#Exclude individuals with an event before indexdate
	if (exclusion=="outcome before indexdate") {
		temp <- temp |> fsubset(dementia==FALSE)
	} else if (exclusion=="consultation in year before indexdate") {
	  temp <- temp |> fsubset(dementia==FALSE) |> fsubset(cons_in_year_pre_index==TRUE)
	} else {temp <- temp |> ftransform(obsdate=if_else(obsdate<indexdate, NA_Date_, obsdate))}
	
	#Set enddate and remove patients without follow-up time
	temp <- temp |>
		ftransform(enddate=pmin(enddate, obsdate, na.rm = TRUE)) |> 
		ftransform(event=if_else(obsdate==enddate, 1, 0, missing=0)) |> 
		fsubset(enddate>indexdate)
	
	#Remove matched sets that have become incomplete (i.e. no longer containing at least 1 exposed and 1 unexposed person)
	complete_sets <- temp |> 
		fgroup_by(setid) |> 
		get_vars("exposed") |> 
		fmean() |> 
		fsubset(exposed!=0 & exposed !=1) |> 
		pull(setid)
	
	cohort_post_exclusion <- temp |> 
	  fsubset(setid %in% complete_sets) |> 
	  ftransform(indexdate_copy=indexdate) #Make copy because variable will be deleted when used in SurvSplit
}

create_cohort_severity <- function(cohort_post_exclusion, psoriasis_severity, psoriatic_arthritis_eventdata) {
  
  # prepare
  exposed <- cohort_post_exclusion |> fsubset(exposed==1)
  mod_sev <- 	exposed[c("patid", "indexdate")] |>
    join(psoriasis_severity, on="patid", how="left", verbose=0, multiple=TRUE) |> 
    fsubset(eventdate >= indexdate) # Only events indicating moderate or severe that occur on or after indexdate
  mod_sev$psoriasis_severity <- 2
  mod_sev <- mod_sev |> 
    roworder(eventdate) |> 
    fgroup_by(patid) |> 
    ffirst()
  
  # tmerge psoriasis severity
  exposed <- tmerge(exposed, exposed, id=patid, tstart = indexdate, tstop = enddate)
  exposed <- tmerge(exposed, exposed, id=patid, event=event(tstop, event))
  exposed <- tmerge(exposed, mod_sev, id=patid, psoriasis_severity=tdc(eventdate, psoriasis_severity, 1))
  exposed <- tmerge(exposed, psoriatic_arthritis_eventdata, id=patid, psoriatic_arthritis=tdc(obsdate))
  
  # set psoriasis severity to 0 in unexposed
  unexposed <- cohort_post_exclusion |> 
    fsubset(exposed==0) |> 
    ftransform(psoriasis_severity=0) |> 
    ftransform(event=event, tstart=indexdate, tstop=enddate)
  
  # bind and make factors
  bind_rows(exposed, unexposed) |> 
    ftransform(psoriasis_severity=factor(psoriasis_severity, levels=c(0,1,2), labels=c("no psoriasis", "mild", "moderate_to_severe"))) |> 
    ftransform(time_in_study=as.numeric(tstop-indexdate))
  
}

create_cohort_split <- function(cohort_post_exclusion) {
  
  #Split by calendar time
  splt <- survSplit(Surv(time = as.numeric(indexdate_copy),
                         time2 = as.numeric(enddate),
                         origin = 0,
                         event = event) ~ ., data=cohort_post_exclusion, 
                    cut=c(34*365.25, 42*365.25, 44*365.25, 46*365.25, 50*365.25), episode="calendar_period")
  
  #Make factors
  splt$calendar_period <- factor(splt$calendar_period,levels = c(1, 2, 3, 4, 5, 6),
                                 labels = c("1997–2003", "2004–2011", "2012–2013", "2014–2015", "2016–2019", "2020–2021"))
  
  splt
}

create_cohort_split_severity <- function(cohort_severity) {
  
  #Split by calendar time
  splt <- survSplit(Surv(time = as.numeric(tstart),
                         time2 = as.numeric(tstop),
                         origin = 0,
                         event = event) ~ ., data=cohort_severity, 
                    cut=c(34*365.25, 42*365.25, 44*365.25, 46*365.25, 50*365.25), episode="calendar_period")
  
  #Make factors
  splt$calendar_period <- factor(splt$calendar_period,levels = c(1, 2, 3, 4, 5, 6),
                                 labels = c("1997–2003", "2004–2011", "2012–2013", "2014–2015", "2016–2019", "2020–2021"))
  
  splt
}
	
analysis_rates <- function(outcome, cohort_post_exclusion, exclusion, cohort_labels, model, exposure) {
	  
  #Exclude rows with missing values for any variable
	  vars <- str_split_1(model, " \\+ | \\* ") |> str_subset(".+") 
	  vars <- vars[vars %in% names(cohort_post_exclusion)]
	  temp <- cohort_post_exclusion |> filter(if_all(vars, \(x) !is.na(x)))
	  
	  
	  if (exposure=="exposed") {
	    survival_object <- Surv(time = as.numeric(temp$indexdate),
	                            time2 = as.numeric(temp$enddate),
	                            origin = as.numeric(temp$indexdate),
	                            event = temp$event)
	  } else {
	    survival_object <- Surv(time = as.numeric(temp$tstart),
	                            time2 = as.numeric(temp$tstop),
	                            origin = as.numeric(temp$indexdate),
	                            event = temp$event)
	  }

	  fit <- survfit(formula(paste("survival_object ~", exposure)), data = temp)
	  
	  
	  # Get person years
	  results_rates <- pyears(formula(paste("survival_object ~", exposure)), 
	                          data = temp, 
	                          scale = 365.25) |>
	    tidy() |> 
	    mutate(rate=(event/pyears)*1000) |> 
	    bind_cols(tibble(term=paste0(exposure, levels(as.factor(temp[[exposure]]))), 
	                 cohort=cohort_labels,
	                 outcome=outcome,
	                 model=names(model),
	                 exclusion=exclusion)) |> 
	    bind_cols(as.data.frame(quantile(fit), row.names=NULL))
	
	#Get unexposed person years in the same row
	results_rates |> 
		group_by(cohort, outcome, model, exclusion) |>
		mutate(pyears_unexposed=pyears[1],
					 n_unexposed=n[1],
					 event_unexposed=event[1],
					 rate_unexposed=rate[1],
					 ratio=1/(pyears[2]/pyears[1]),
					 ratio_text=paste0("1:", round(ratio))) |>
		ungroup() 

}
	
analysis_hrs <- function(outcome, cohort_split, cohort_labels, exclusion, exposure, model) {
  
  #Create survival object
  survival_object <- Surv(time = cohort_split$tstart,
                          time2 = cohort_split$tstop,
                          origin = as.numeric(cohort_split$indexdate),
                          event = cohort_split$event)  
  
  survival::coxph(formula(paste("survival_object ~", exposure, model, " + strata(setid) + cluster(patid)")), 
                  data=cohort_split, id=patid) |>
    broom::tidy(exponentiate=TRUE, conf.int=TRUE) |> 
    dplyr::mutate(cohort=cohort_labels,
                  exposure=exposure,
                  outcome=outcome,
                  model=names(model),
                  exclusion=exclusion
    )
}

  

create_cohort_long <- function(cohort_wide, codelists, eventdata) {
  cli_alert_info("Starting t-merging")

cohort_wide <- cohort_wide |> 
    select(-matches(codelists$name))
  
# Exposed
exposed <- cohort_wide |> 
  fsubset(exposed==1)

exposed <- tmerge(exposed, exposed, id=patid, tstart = indexdate, tstop = enddate)

cli::cli_progress_bar("t-merging exposed", total = nrow(codelists), type="tasks")
for (i in seq_along(eventdata)) {
  exposed <- tmerge(exposed, eventdata[[i]], id=patid, time_dep_var=tdc(obsdate))
  exposed <- exposed |> rename_with(\(x) codelists$name[[i]], time_dep_var)
  cli::cli_progress_update()
}

# Unexposed
unexposed <- cohort_wide |> 
  fsubset(exposed==0)

unexposed <- tmerge(unexposed, unexposed, id=patid, tstart = indexdate, tstop = enddate)

cli::cli_progress_bar("t-merging unexposed", total = nrow(codelists), type="tasks")
for (i in seq_along(eventdata)) {
  unexposed <- tmerge(unexposed, eventdata[[i]], id=patid, time_dep_var=tdc(obsdate))
  unexposed <- unexposed |> rename_with(\(x) codelists$name[[i]], time_dep_var)
  cli::cli_progress_update()
}

bind_rows(exposed, unexposed)
}


create_cohort_post_exclusion_long <- function(outcome, eventdata, hes_eventdata, cohort_long, cohort_labels_long) {

  # Exclude individuals with dementia before or on indexdate ----------------
  temp <- cohort_long |>
    fgroup_by(patid, exposed) |> 
    fmutate(exclude=any(dementia[tstart<=indexdate]==1)) |> 
    fungroup() |> 
    fsubset(exclude==FALSE)
  
  #Remove matched sets that have become incomplete (i.e. no longer containing at least 1 exposed and 1 unexposed person)
  complete_sets <- temp |> 
    fgroup_by(setid) |> 
    get_vars("exposed") |> 
    fmean() |> 
    fsubset(exposed!=0 & exposed !=1) |> 
    pull(setid)
  cohort_post_exclusion <- temp |> fsubset(setid %in% complete_sets)
  
  # Add events --------------------------------------------------------------
  #Get events
  events <- hes_eventdata |> 
    select(patid, obsdate=epistart) |> 
    bind_rows(eventdata[c("patid", "obsdate")]) |> 
    roworder(obsdate) |> 
    collapse::funique.data.frame() #Only keep one row if there are multiple events for the same patient on the same day
  
  exposed <- cohort_post_exclusion |> fsubset(exposed==1)
  unexposed <- cohort_post_exclusion |> fsubset(exposed==0)
  
  #Manually assign tmerge attributes
  attributes(exposed)$class <- c("tmerge", "data.frame")
  attributes(exposed)$tm.retain$tname$idname <- "patid"
  attributes(exposed)$tm.retain$tname$tstartname <- "tstart"
  attributes(exposed)$tm.retain$tname$tstopname <- "tstop"
  attributes(exposed)$tm.retain$n <- nrow(exposed)
  
  attributes(unexposed)$class <- c("tmerge", "data.frame")
  attributes(unexposed)$tm.retain$tname$idname <- "patid"
  attributes(unexposed)$tm.retain$tname$tstartname <- "tstart"
  attributes(unexposed)$tm.retain$tname$tstopname <- "tstop"
  attributes(unexposed)$tm.retain$n <- nrow(unexposed)

  #tmerge events
  exposed <- tmerge(exposed, events, id=patid, event=event(obsdate))
  unexposed <- tmerge(unexposed, events, id=patid, event=event(obsdate))
  
  bind_rows(exposed, unexposed)
}
  

analysis_long <- function(outcome, cohort_long_split, cohort_labels_long, exposure, model, n_outcomes) {

  #Create survival object
  survival_object <- Surv(time = cohort_long_split$tstart,
                          time2 = cohort_long_split$tstop,
                          origin = as.numeric(cohort_long_split$indexdate),
                          event = cohort_long_split$event)
  
  #Analysis rates
    vars <- str_split_1(model, " \\+ | \\* ") |> str_subset(".+") 
    
    cohort_long_split_filtered <- cohort_long_split |> filter(if_all(vars, \(x) !is.na(x)))
    
    results_rates <- pyears(Surv(time = cohort_long_split_filtered$tstart,
                time2 = cohort_long_split_filtered$tstop,
                origin = as.numeric(cohort_long_split_filtered$indexdate),
                event = cohort_long_split_filtered$event) ~ exposed, data = cohort_long_split_filtered, scale = 365.25) |>
      tidy() |> 
      mutate(rate=(event/pyears)*1000) |> 
      cbind(tibble(term=c("unexposed", "exposed"), 
                   cohort=cohort_labels_long,
                   outcome=outcome,
                   model=names(model),
                   exclusion="outcome before indexdate")) 
  
  #Get unexposed person years in the same row
  results_rates <- results_rates |> 
    group_by(cohort, outcome, model, exclusion) |>
    mutate(pyears_unexposed=pyears[1],
           n_unexposed=n[1],
           event_unexposed=event[1],
           rate_unexposed=rate[1],
           ratio=1/(pyears[2]/pyears[1]),
           ratio_text=paste0("1:", round(ratio))) |>
    ungroup()
  
  
  #analysis_regression
  results_regression <- survival::coxph(formula(paste("survival_object ~", "exposed", model, " + strata(setid) + cluster(patid)")), 
                                                data=cohort_long_split, id=patid) |>
                                  broom::tidy(exponentiate=TRUE, conf.int=TRUE) |> 
                                  dplyr::mutate(cohort=cohort_labels_long,
                                                exposure=exposure,
                                                outcome=outcome,
                                                model=names(model),
                                                exclusion="outcome before indexdate",
                                                bonf=ifelse(p.value<(0.05/n_outcomes), "*", ""), #Report if p value is smaller than bonferroni-corrected alpha
                                                # estimate=1,conf.low=0.9, conf.high=1.1, bonf="*") #DUMMY RESULTS
                                  )
  
  results_regression |> 
    left_join(results_rates, by=c("cohort", "outcome", "model", "exclusion", "term"))
  
  
}
 



#' Calculate participant counts at various stages
#' @param files 
#' @param extract 
#' @param extract_ocs 
#' @return
analysis_cohort_flow <- function(denominator, cohort_defined, cohort_labels, cohort_matched, cohort_eligible) {
	db_pop <- denominator |> 
		nrow() |> 
		as_tibble() |> 
		mutate(step="database population") |> 
		rename(n=value)
	
	defined_pop <- cohort_defined |> 
		filter(!is.na(exposed_date)) |> 
		count() |> 
		mutate(step="who ever have psoriasis")
	
	cohort_matched_pop <- cohort_matched |> 
		count() |> 
		mutate(step="with psoriasis matched to people without Eczema")
	
	cohort_eligible_pop <- cohort_eligible |> 
		count() |> 
		mutate(step="eligible for matching")
	
	bind_rows(db_pop, defined_pop, cohort_eligible_pop, cohort_matched_pop) |> 
		mutate(cohort=cohort_labels)
	
}

analysis_baseline_chars <- function(cohort_wide, denominator, cohort_labels) {
  cohort_wide |>
    join(denominator, on="patid", how="left", multiple = FALSE) |> 
    mutate(N=1, 
           indexdate_num=as.numeric(indexdate), 
           enddate_num=as.numeric(enddate),
           age_at_index=year(indexdate)-yob,
           futime_baseline=(enddate_num-indexdate_num)/365.25) |> 
    mutate(agegroup=case_when(
      age_at_index >= 40 & age_at_index < 50 ~ "40-49",
      age_at_index >= 50 & age_at_index < 60 ~ "50-59",
      age_at_index >= 60 & age_at_index < 70 ~ "60-69",
      age_at_index >= 70 & age_at_index < 80 ~ "70-79",
      age_at_index >= 80 & age_at_index < 90 ~ "80-89",
      age_at_index >= 90 & age_at_index < 100 ~ "90-99",
      TRUE ~ "100+"
    )) |> 
    mutate(across(where(is.character), as_factor)) |> 
    mutate(across(where(is.factor), fct_drop)) |> 
    mutate(across(where(is.factor), \(x) fct_explicit_na(x, "missing"))) |> 
    mutate(across(where(is.factor), ~ factor(.x, labels = paste(cur_column(), levels(.x), sep = " ")))) |> 
    select(N, gender, age_at_index, yob, region, futime_baseline, everything(), -patid, -setid) |> 
    tbl_summary(by=exposed,
                statistic=list(futime_baseline ~ "{median} ({p25}, {p75}) [{sum}]")) |>
    modify_header(all_stat_cols() ~ "{level}") |> 
    as_tibble() |> 
    mutate(cohort=cohort_labels)
}



analysis_interactions <- function(outcome, cohort_post_exclusion, cohort_labels, exclusion, exposure) {
  
  # Create time-updated splits with follow-up time and exposed
  cohort_split_futime <- survSplit(Surv(time = as.numeric(indexdate_copy),
                                        time2 = as.numeric(enddate),
                                        origin = as.numeric(indexdate_copy),
                                        event = event) ~ .,
                                   data=cohort_post_exclusion,
                                   cut=c((365.25*5)*1:6), episode="futime")
  
  fit <- coxph(Surv(tstart, tstop, event) ~ exposed:factor(futime) + strata(setid) + cluster(patid) + imd + chronic_liver_disease + chronic_lung_disease + cardiovascular_disease + cerebrovascular_disease + diabetes + depression + cholesterol + hypertension + chronic_kidney_disease,
               data = cohort_split_futime)
  
  # Get person years
  rates <- map(c(1:5), ~pyears(Surv(tstart, tstop, event) ~ exposed, 
                               data = filter(cohort_split_futime, futime==.x), 
                               scale = 365.25) |>
                 tidy() |> 
                 mutate(rate=(event/pyears)*1000, name=c("_unexposed", "")) |> 
                 pivot_wider(names_from = name, values_from = c(everything(), -name), names_sep="") |> 
                 mutate(futime=.x)) |> 
    list_rbind()
  
  fit |> 
    tidy(conf.int = TRUE, exponentiate = TRUE) |> 
    filter(str_starts(term, "exposed")) |> 
    bind_cols(rates) |> 
    mutate(cohort=cohort_labels,
                  exposure=exposure,
                  outcome=outcome,
                  exclusion=exclusion)
  
}



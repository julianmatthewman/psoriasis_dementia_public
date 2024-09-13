alg_psoriasis_severity <- function(exposed_obs_defined, exposed_drug_defined, codelists_for_define, hes_diagnosis_psoriasis) {
  eventdata_phototherapy <- exposed_obs_defined |> 
    collapse::fsubset(medcodeid %in% codelists_for_define$codes[[which(codelists_for_define$name=="phototherapy")]]) |> 
    rename(eventdate=obsdate) |> 
    select(patid, eventdate)
  eventdata_sys_trt <- exposed_drug_defined |> 
    collapse::fsubset(prodcodeid %in% codelists_for_define$codes[[which(codelists_for_define$name=="systemic_treatments_for_psoriasis")]]) |> 
    rename(eventdate=issuedate) |> 
    select(patid, eventdate)
  
  bind_rows(
    eventdata_phototherapy,
    eventdata_sys_trt,
    select(hes_diagnosis_psoriasis, patid, eventdate=obsdate)
  ) |> unique.data.frame()
  
}

alg_polypharmacy <- function(cohort_matched, drug_files, aurum_product) {
  
  temp <- open_dataset(drug_files) |> 
    select(patid, issuedate, prodcodeid) |> 
    left_join(aurum_product[c("prodcodeid", "bnfchapter")], by="prodcodeid") |> 
    mutate(issuedate=as.Date(issuedate, "%d/%m/%Y")) |> 
    filter(!is.na(bnfchapter) & bnfchapter!="") |> 
    right_join(cohort_matched[c("patid", "exposed","indexdate")]) |> 
    filter(issuedate<=indexdate) |> 
    filter(as.integer(indexdate)-as.integer(issuedate)<365.25) |> 
    group_by(patid, exposed) |> 
    summarise(bnfchapter=n_distinct(bnfchapter)) |>
    ungroup() |> 
    filter(bnfchapter>5) |>
    collect()
  
  cohort_matched |> 
    left_join(temp, by=c("patid", "exposed")) |> 
    mutate(polypharmacy=!is.na(bnfchapter)) |> 
    select(polypharmacy)
  
}

alg_cons_in_year_pre_index <- function(cohort_matched, files, aurum_medical) {
  
  temp <- open_dataset(files) |> 
    select(patid, obsdate) |> 
    mutate(obsdate=as.Date(obsdate, "%d/%m/%Y")) |> 
    right_join(cohort_matched[c("patid", "exposed","indexdate")]) |>
    filter(obsdate<=indexdate) |> 
    filter(as.integer(indexdate)-as.integer(obsdate)<365.25) |> 
    distinct() |> 
    group_by(patid, exposed) |> 
    summarise(n_cons_in_year_pre_index=n()) |> 
    collect() |> 
    mutate(cons_in_year_pre_index=TRUE)
  
  cohort_matched |> 
    left_join(temp, by=c("patid", "exposed")) |> 
    mutate(cons_in_year_pre_index=!is.na(cons_in_year_pre_index)) |> 
    mutate(n_cons_in_year_pre_index=if_else(is.na(n_cons_in_year_pre_index), 0, n_cons_in_year_pre_index)) |> 
    select(cons_in_year_pre_index, n_cons_in_year_pre_index)
  
}


alg_alcoholism <- function(cl, files, drug_files, study_start, study_end) {
  alc_medcodes <- unlist(cl$codes[cl$name=="alcohol_abuse"])
  alc_prodcodes <- unlist(cl$codes[cl$name=="alcoholism_drugs"])
  
  alc_obs <- open_dataset(files) |> 
    select(patid, obsdate, medcodeid) |> 
    filter(medcodeid %in% alc_medcodes) |> 
    mutate(obsdate=as.Date(obsdate, "%d/%m/%Y")) |> 
    mutate(obsdate>=study_start & obsdate<=study_end) |> 
    arrange(obsdate) |> 
    collect()

  alc_drugs <- open_dataset(drug_files) |> 
    select(patid, issuedate, prodcodeid) |> 
    filter(prodcodeid %in% alc_prodcodes) |> 
    mutate(obsdate=as.Date(issuedate, "%d/%m/%Y")) |> 
    mutate(obsdate>=study_start & obsdate<=study_end) |> 
    arrange(issuedate) |> 
    collect()
  
  bind_rows(alc_obs, alc_drugs) |> 
    select(patid, obsdate, medcodeid)
  
}


alg_sleep <- function(cl, files, drug_files, study_start, study_end) {
  alc_medcodes <- unlist(cl$codes[cl$name=="sleep_disorders"])
  alc_prodcodes <- unlist(cl$codes[cl$name=="sleep_drugs"])
  
  sleep_obs <- open_dataset(files) |> 
    select(patid, obsdate, medcodeid) |> 
    filter(medcodeid %in% alc_medcodes) |> 
    mutate(obsdate=as.Date(obsdate, "%d/%m/%Y")) |> 
    mutate(obsdate>=study_start & obsdate<=study_end) |> 
    arrange(obsdate) |> 
    collect()
  
  sleep_drugs <- open_dataset(drug_files) |> 
    select(patid, issuedate, prodcodeid) |> 
    filter(prodcodeid %in% alc_prodcodes) |> 
    mutate(obsdate=as.Date(issuedate, "%d/%m/%Y")) |> 
    mutate(obsdate>=study_start & obsdate<=study_end) |> 
    arrange(issuedate) |> 
    collect()
  
  bind_rows(sleep_obs, sleep_drugs) |> 
    select(patid, obsdate, medcodeid)
  
}


alg_ckd <- function(chronic_kidney_disease_codelist, serum_creatinine_codelist, files, drug_files, study_start, study_end, numunit, denominator) {
  ckd_medcodes <- unlist(chronic_kidney_disease_codelist$codes)
  crea_medcodes <- unlist(serum_creatinine_codelist$codes)
  
  ckd_obs <- open_dataset(files) |> 
    select(patid, obsdate, medcodeid) |> 
    filter(medcodeid %in% ckd_medcodes) |> 
    mutate(obsdate=as.Date(obsdate, "%d/%m/%Y")) |> 
    mutate(obsdate>=study_start & obsdate<=study_end) |> 
    arrange(obsdate) |> 
    collect()
  
  crea <- open_dataset(files) |> 
    select(patid, obsdate, medcodeid, numunitid, value) |> 
    filter(medcodeid %in% crea_medcodes) |> 
    mutate(obsdate=as.Date(obsdate, "%d/%m/%Y")) |> 
    mutate(obsdate>=study_start & obsdate<=study_end) |> 
    arrange(obsdate) |> 
    collect() |>
    left_join(numunit, by="numunitid")
  
  egfr_sub_60_obs <- crea |> 
    fsubset(Description %in% c("umol/L", "micmol/l", "micromol/L")) |> 
    join(denominator[c("patid", "gender", "yob")], multiple=TRUE) |> 
    ftransform(dob=as.Date(paste0(yob, "-06-01"))) |>
    ftransform(age=as.numeric(obsdate-dob)/365.25) |> 
    ftransform(scr=value*0.0113) |> # Convert to mg/dl
    ftransform(kappa=ifelse(gender=="F", 0.7, 0.9)) |>
    ftransform(alpha=ifelse(gender=="F", -0.241, -0.302)) |>
    ftransform(iffemale=ifelse(gender=="F", 1.012, 1)) |>
    ftransform(minscrk1=pmin(scr/kappa, 1)) |> 
    ftransform(maxscrk1=pmax(scr/kappa, 1)) |> 
    ftransform(egfr=142 * (minscrk1^alpha) * (maxscrk1^-1.2) * (0.9938^age) * iffemale) |> 
    fsubset(egfr<60 & egfr>1) |> 
    fselect(patid, obsdate, medcodeid)

  bind_rows(ckd_obs, egfr_sub_60_obs) |> 
    select(patid, obsdate, medcodeid)
  
}


# BMI and smoking status were defined pragmatically by using the status recorded closest to cohort entry date 
# (with records: within the year before to 1 month after cohort entry date was regarded as best, 
#   from 1 month to 1 year after cohort entry was regarded as second best, 
#   the most recent record prior to 12 months before cohort entry date was regarded as third best.
# Example data to test:
# tibble(patid=c(1,1,1,1), 
#        exposed=c(1,1,1,1),
#        indexdate=as_date(c("2000-01-01", "2000-01-01", "2000-01-01", "2000-01-01")),
#        obsdate=as.Date(c("2000-01-02", "1999-06-01", "1990-01-01", "2020-01-01")),
#        smokstatus=c(1,2,3,4))
alg_smoking <- function(cohort_matched, smok_eventdata) {
  smok <- cohort_matched |> 
    join(smok_eventdata, on="patid", how="left", multiple=TRUE) |> 
    ftransform(distance=as.numeric(indexdate-obsdate)) |> 
    ftransform(abs_distance=abs(distance)) |> 
    roworder(abs_distance) |> 
    fgroup_by(patid, exposed) |> 
    fsummarise(smoking_status=ifelse(any(distance < 365 & distance > -31), first(smokstatus[distance < 365 & distance > -31]),
                            ifelse(any(distance >= -31 & distance < -365), first(smokstatus[distance >= -31 & distance < -365]),
                                   ifelse(any(distance > 0), first(smokstatus[distance > 0]), NA)))) |> 
    fungroup() |> 
    ftransform(smoking_status=factor(smoking_status, levels=c("non-smoker", "current smoker", "ex-smoker", "current or ex-smoker"))) |> 
    ftransform(smoking_status=fct_collapse(smoking_status, non_smoker="non-smoker", smoker=c("current smoker", "ex-smoker", "current or ex-smoker")))

  cohort_matched |> 
    left_join(smok, by=c("patid", "exposed")) |> 
    select(smoking_status) |>
    ftransform(smoking_status_na=smoking_status) |> 
    ftransform(smoking_status=fct_na_value_to_level(smoking_status, level="non_smoker"))
  
  }


alg_bmi <- function(cohort_matched, bmi_eventdata) {
  height_in_cm <- bmi_eventdata |> fsubset(numunitid == 122) |> fsubset(value>100 & value < 250)
  height_in_cm$value |> summary()
  height_in_m <- bmi_eventdata |> fsubset(numunitid == 173) |> fsubset(value>1 & value < 2.5)
  height_in_m$value |> summary()
  height_in_m <- bind_rows(height_in_m, mutate(height_in_cm, value=value/100))
  height_in_m$value |> summary()
  
  weight_in_kg <- bmi_eventdata |> fsubset(numunitid %in% c(156, 827)) |> fsubset(value>30 & value <300)
  weight_in_kg$value |> summary()
  
  bmi_recorded <- bmi_eventdata |> fsubset(numunitid %in% c(157, 657, 907, 108, 568, 1309)) |> fsubset(value>10 & value <60)
  bmi_recorded$value |> summary()
  
  #Use the closest height recording to indexdate
  height <- cohort_matched[c("patid", "exposed", "indexdate")] |> 
    join(height_in_m[c("patid", "value", "obsdate")], on="patid", how="left", multiple=TRUE) |> 
    ftransform(distance=as.numeric(indexdate-obsdate)) |> 
    ftransform(abs_distance=abs(distance)) |> 
    roworder(abs_distance) |> 
    fgroup_by(patid) |> #Assume height stays the same so no grouping by exposed
    ffirst() |> 
    fselect(patid, height_in_m=value)
  
  #Pick weight recording as was done for smoking
  weight <- cohort_matched[c("patid", "exposed", "indexdate")] |> 
    join(weight_in_kg[c("patid", "value", "obsdate")], on="patid", how="left", multiple=TRUE) |> 
    ftransform(distance=as.numeric(indexdate-obsdate)) |> 
    ftransform(abs_distance=abs(distance)) |> 
    roworder(abs_distance) |> 
    fgroup_by(patid, exposed) |> 
    fsummarise(value=ifelse(any(distance < 365 & distance > -31), first(value[distance < 365 & distance > -31]),
                            ifelse(any(distance >= -31 & distance < -365), first(value[distance >= -31 & distance < -365]),
                                   ifelse(any(distance > 0), first(value[distance > 0]), NA)))) |> 
    fungroup() |> 
    fselect(patid, exposed, weight_in_kg=value)
  
  #Pick BMI recording as was done for smoking
  bmi_recorded <- cohort_matched[c("patid", "exposed", "indexdate")] |>
    join(bmi_recorded[c("patid", "value", "obsdate")], on="patid", how="left", multiple=TRUE) |>
    ftransform(distance=as.numeric(indexdate-obsdate)) |>
    ftransform(abs_distance=abs(distance)) |>
    roworder(abs_distance) |>
    fgroup_by(patid, exposed) |> 
    fsummarise(value=ifelse(any(distance < 365 & distance > -31), first(value[distance < 365 & distance > -31]),
                            ifelse(any(distance >= -31 & distance < -365), first(value[distance >= -31 & distance < -365]),
                                   ifelse(any(distance > 0), first(value[distance > 0]), NA)))) |> 
    fungroup() |> 
    select(patid, exposed, bmi_recorded=value)
  
  bmi_calc <- height |> 
    join(weight, on=c("patid"), how="left", multiple=TRUE) |> 
    ftransform(bmi_calc=weight_in_kg /(height_in_m^2)) |> 
    fsubset(!is.na(bmi_calc)) |> 
    fselect(patid, exposed, bmi_calc)
  
  bmi_final <- full_join(bmi_recorded,bmi_calc, by=c("patid", "exposed")) |>
    mutate(bmi=coalesce(bmi_calc, bmi_recorded)) |>
    select(patid, exposed, bmi)
  
  cohort_matched |> 
    left_join(bmi_final, by=c("patid", "exposed")) |> 
    ftransform(obese=as_factor(if_else(bmi<25 | is.na(bmi), "not obese", "obese")),
               obese_na=as_factor(if_else(bmi<25, "not obese", "obese"))) |> 
    fselect(obese, obese_na)
  }


alg_ethnicity <- function(codelists_for_define, files) {
  library(data.table)
  ethcodes <- unlist(codelists_for_define$codes[codelists_for_define$name=="ethnicity"])
  ethcodelist <- codelists_for_define$full[codelists_for_define$name=="ethnicity"][[1]]
  
  
  ethn_recs <- open_dataset(files) |> 
    select(patid, obsdate, medcodeid) |> 
    filter(medcodeid %in% ethcodes) |> 
    collect() |> 
    join(ethcodelist[c("medcodeid","eth5","eth16")], on="medcodeid", multiple=TRUE)
  
  
  #tabyl(ethn_recs$eth16)
  
  df <- as.data.table(ethn_recs)
  
  #tagging patients with the same ethnicity entry within the same year
  df<- df[order(patid)]
  df <- df[, sysyear := year(obsdate)]
  df[, duplicates := .N, by = c("patid", "sysyear", "medcodeid")]
  #keeping the first ethnicity entry per year if there is a duplicate
  df[duplicates > 1, first_entry := lapply(.SD, min), by = c("patid", "sysyear"),
     .SDcols = "obsdate"]
  df[duplicates >1, keep_row := obsdate == first_entry]
  df[duplicates == 1, keep_row := TRUE]
  df <- df[keep_row == TRUE]
  
  df[, duplicates := NULL]
  df[, first_entry := NULL]
  df[, keep_row := NULL]
  
  
  #count the observations of ethnicity by patient
  df <- df[, by = c("patid", "eth5"), eth5_count := .N]
  
  tmp <- df[, list(patid, eth5_count)]
  tmp <- unique(tmp)
  #janitor::tabyl(tmp$eth5_count) #proportion of patients with certain number of 
  #ethnicity records
  tmp <- NULL
  
  
  ###adding up ethnicity - 5 categories and 16 categories
  df[eth5 == "0. White", eth5whitecount := .N, by = "patid"]
  df[is.na(eth5whitecount), eth5whitecount := 0]
  df[eth5 == "1. South Asian", eth5sacount := .N, by = "patid"]
  df[is.na(eth5sacount), eth5sacount := 0]
  df[eth5 == "2. Black", eth5blackcount := .N, by = "patid"]
  df[is.na(eth5blackcount), eth5blackcount := 0]
  df[eth5 == "3. Other", eth5othercount := .N, by = "patid"]
  df[is.na(eth5othercount), eth5othercount := 0]
  df[eth5 == "4. Mixed", eth5mixedcount := .N, by = "patid"]
  df[is.na(eth5mixedcount), eth5mixedcount := 0]
  df[eth5 == "5. Not Stated", notstated5count := .N, by = "patid"]
  df[is.na(notstated5count), notstated5count := 0]
  
  
  df[eth16 =="1. British", british16count := .N, by = "patid"]
  df[is.na(british16count), british16count := 0]
  df[eth16 =="2. Irish", irish16count := .N, by = "patid"]
  df[is.na(irish16count), irish16count := 0]
  df[eth16 =="3. Other White", otherwhite16count := .N, by = "patid"]
  df[is.na(otherwhite16count), otherwhite16count := 0]
  df[eth16 =="4. White and Black Caribbean", whitecarib16count := .N, by = "patid"]
  df[is.na(whitecarib16count), whitecarib16count := 0]
  df[eth16 =="5. White and Black African", whiteaf16count := .N, by = "patid"]
  df[is.na(whiteaf16count), whiteaf16count := 0]
  df[eth16 =="6. White and Asian", whiteasian16count := .N, by = "patid"]
  df[is.na(whiteasian16count), whiteasian16count := 0]
  df[eth16 =="7. Other Mixed", othermixed16count := .N, by = "patid"]
  df[is.na(othermixed16count), othermixed16count := 0]
  df[eth16 =="8. Indian", indian16count := .N, by = "patid"]
  df[is.na(indian16count), indian16count := 0]
  df[eth16 =="9. Pakistani", pak16count := .N, by = "patid"]
  df[is.na(pak16count), pak16count := 0]
  df[eth16 =="10. Bangladeshi", bangla16count := .N, by = "patid"]
  df[is.na(bangla16count), bangla16count := 0]
  df[eth16 =="11. Other Asian", otherasian16count := .N, by = "patid"]
  df[is.na(otherasian16count), otherasian16count := 0]
  df[eth16 =="12. Caribbean", carib16count := .N, by = "patid"]
  df[is.na(carib16count), carib16count := 0]
  df[eth16 =="13. African", african16count := .N, by = "patid"]
  df[is.na(african16count), african16count := 0]
  df[eth16 =="14. Other Black", otherblack16count := .N, by = "patid"]
  df[is.na(otherblack16count), otherblack16count := 0]
  df[eth16 =="15. Chinese", chinese16count := .N, by = "patid"]
  df[is.na(chinese16count), chinese16count := 0]
  df[eth16 =="16. Other ethnic group", other16count := .N, by = "patid"]
  df[is.na(other16count), other16count := 0]
  df[eth16 =="17. Not Stated", notstated16count := .N, by = "patid"]
  df[is.na(notstated16count), notstated16count := 0]
  
  
  
  #flagging those patients who only have non stated ethnicity
  df<- df[,  nonstatedonly := (notstated5count > 0 &
                                 eth5whitecount == 0 &
                                 eth5sacount == 0 &
                                 eth5blackcount == 0 &
                                 eth5othercount == 0 &
                                 eth5mixedcount == 0)]
  
  
  ###finding the most common ethnicity, excluding ethnicity not stated
  # Get Row wise max in R
  df <- df[,eth5max_f :=  names(.SD)[max.col(.SD, ties.method = "first")], .SDcols = 8:12] #the eth5 columns
  df <- df[,eth5max_l :=  names(.SD)[max.col(.SD, ties.method = "last")], .SDcols = 8:12]
  df <- df[,eth16max_f :=  names(.SD)[max.col(.SD, ties.method = "first")], .SDcols = 14:29]
  df <- df[,eth16max_l :=  names(.SD)[max.col(.SD, ties.method = "last")], .SDcols = 14:29]
  
  #get the rows with equally common ethnicities, excluding non stated
  df<- df[, equaleth5 := eth5max_f != eth5max_l & notstated5count == 0 ]
  df<- df[, equaleth16 := eth16max_f != eth16max_l & notstated16count  ==0]
  #janitor::tabyl(df$equaleth5) #how many people with equally common ethnicity
  
  
  
  ###---FINAL STEP - assigning ethnicity to patid
  #case 1: ethnicity is not stated - assign not stated
  eth <- df[nonstatedonly==T, ethnicity_5 := 5]
  eth <- df[nonstatedonly==T, ethnicity_16 := 17]
  
  tmp <- eth[nonstatedonly == T]
  #length(unique(tmp$patid))
  
  
  ####case 2: assign the most common ethnicity
  eth[equaleth5 == F & nonstatedonly == F, ethnicity_5c := eth5max_f]
  eth[equaleth16 == F & nonstatedonly == F, ethnicity_16c := eth16max_f]
  
  #intermediate step, translating the character back into ethnicity numbers
  eth[ethnicity_5c == "eth5whitecount", ethnicity_5 := 0]
  eth[ethnicity_5c == "eth5sacount", ethnicity_5 := 1]
  eth[ethnicity_5c == "eth5blackcount", ethnicity_5 := 2]
  eth[ethnicity_5c == "eth5othercount", ethnicity_5 := 3]
  eth[ethnicity_5c == "eth5mixedcount", ethnicity_5 := 4]
  
  eth[ethnicity_16c ==  "british16count", ethnicity_16 := 1]
  eth[ethnicity_16c ==  "irish16count", ethnicity_16 := 2]
  eth[ethnicity_16c ==  "otherwhite16count", ethnicity_16 := 3]
  eth[ethnicity_16c ==  "whitecarib16count", ethnicity_16 := 4]
  eth[ethnicity_16c ==  "whiteaf16count", ethnicity_16 := 5]
  eth[ethnicity_16c ==  "whiteasian16count", ethnicity_16 := 6]
  eth[ethnicity_16c ==  "othermixed16count", ethnicity_16 := 7]
  eth[ethnicity_16c ==  "indian16count", ethnicity_16 := 8]
  eth[ethnicity_16c ==  "pak16count", ethnicity_16 := 9]
  eth[ethnicity_16c ==  "bangla16count", ethnicity_16 := 10]
  eth[ethnicity_16c ==  "otherasian16count", ethnicity_16 := 11]
  eth[ethnicity_16c ==  "carib16count", ethnicity_16 := 12]
  eth[ethnicity_16c ==  "african16count", ethnicity_16 := 13]
  eth[ethnicity_16c ==  "otherblack16count", ethnicity_16 := 14]
  eth[ethnicity_16c ==  "chinese16count", ethnicity_16 := 15]
  eth[ethnicity_16c ==  "other16count", ethnicity_16 := 16]
  
  df1 <- eth[equaleth5 == F]
  #length(unique(df1$patid)) # how many patients with most common eth5
  
  df1 <- eth[equaleth16 == F]
  #length(unique(df1$patid)) # how many patients with most common eth16
  
  
  
  ###case 3: if equal common
  #finding the latest date of ethnicity record and using this record as eth5/16 code
  eth[equaleth5 ==T, latestethdate := lapply(.SD, max), by = "patid", .SDcols = "obsdate"]
  eth[equaleth5 == T, eth_pick := obsdate == latestethdate]
  eth[equaleth5 == T & eth_pick == T, ethnicity_5 := eth5]
  
  eth[equaleth16 ==T, latestethdate := lapply(.SD, max), by = "patid", .SDcols = "obsdate"]
  eth[equaleth16 == T, eth_pick := obsdate == latestethdate]
  eth[equaleth16 == T & eth_pick == T, ethnicity_16 := eth16]
  
  
  ###--- creating a clean ethnicity file
  eth <- unique(eth[, list(patid, ethnicity_5, ethnicity_16)])
  #janitor::tabyl(eth$ethnicity_5)
  #janitor::tabyl(eth$ethnicity_16)
  
  
  ###case 4: different ethnicities assigned on the same day
  # assign to "other" ethnicitiy group
  eth[duplicated(patid) | duplicated(patid, fromLast=TRUE), ethnicity_16 := 16]
  eth[duplicated(patid) | duplicated(patid, fromLast=TRUE), ethnicity_5 := 3]
  
  eth <- unique(eth[, list(patid, ethnicity_5, ethnicity_16)])
  #janitor::tabyl(eth$ethnicity_5)
  #janitor::tabyl(eth$ethnicity_16)
  
  
  as_tibble(eth)
}


alg_frailty <- function(cohort_wide) {
  cohort_wide |> 
    rowwise() |> 
    mutate(frailty_n=sum(activity_limitation,arthritis,cerebrovascular_disease, diabetes, dizziness,
                           dyspnoea,foot_problems,fragility_fracture,hearing_loss,heart_failure,
                           heart_valve_disease,housebound,hypertension, hypotension_syncope,ischaemic_heart_disease,
                           mobility_transfer_problems,parkinsonism_tremor,peptic_ulcer,peripheral_vascular_disease,requirement_for_care,
                           respiratory_disease,skin_ulcer,sleep_disorders,social_vulnerability,urinary_incontinence,
                           urinary_system_disease,visual_impairment,anaemia, atrial_fibrillation,falls,
                           thyroid_disease,osteoporosis,weight_loss_anorexia,chronic_kidney_disease, polypharmacy)) |> 
    ungroup() |> 
    mutate(frailty_score=frailty_n/35,
           frailty_score=case_when(frailty_score < 0.12 ~ "fit",
                                     frailty_score < 0.24 ~ "mild frailty",
                                     frailty_score < 0.36 ~ "moderate frailty",
                                     frailty_score >= 0.36 ~ "severe frailty"),
           frailty_score=factor(frailty_score, levels=c("fit", "mild frailty", "moderate frailty", "severe frailty")))
}

alg_high_dose_oral_glucocorticoids <- function(drug_eventdata, drug_codelists, common_dosages) {
  temp <- drug_eventdata[which(drug_codelists$name=="oral_glucocorticoids")][[1]] |> 
    left_join(common_dosages, by="dosageid") |> 
    left_join(drug_codelists[which(drug_codelists$name=="oral_glucocorticoids"),]$full[[1]], by="prodcodeid") |> 
    left_join(read_csv("input/ped_reference.csv"), by="drugsubstancename")
  
  temp2 <- temp |> 
    mutate(dose_in_mg=as.numeric(str_extract(substancestrength, "\\d+(\\.\\d+)?")),
           dose_in_mg=ifelse(str_detect(substancestrength, "micro"), dose_in_mg/1000, dose_in_mg),
           ped=dose_in_mg*(1/conversion_factor),
           daily_ped=as.numeric(daily_dose)*ped)
  
  temp2 |> 
    filter(daily_ped>20)
}

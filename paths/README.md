# `paths/`

Here, in a file named `paths.R` define paths to the data.
If you want to run the pipeline with dummy data, set the following paths:

```
path_extract <- "dummy_data/extract/"
path_denominator <- "dummy_data/denominator/AcceptablePats.parquet"
path_define <- "dummy_data/define/"
path_hes_eligibility <- "dummy_data/linkage/Aurum_enhanced_eligibility_January_2022.dta"
path_linked_data <- "dummy_data/linked_data/"
path_linkage <- "dummy_data/linkage/"
path_extract_ids <- "dummy_data/extract_ids.txt"
```

In addition, you need to provide the path to a folder containing the CPRD Aurum codebrowsers (CPRDAurumMedical.dta and CPRDAurumProduct.dta) in Stata .dta format, and the NumUnit.txt and common_dosages.txt files.
```
path_codebrowsers <- "sensitive_input/CPRD Aurum 2023_03 Code browser/"
path_numunit <- "sensitive_input/NumUnit.txt"
path_common_dosages <- "sensitive_input/common_dosages.txt"
```
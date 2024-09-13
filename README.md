# Code and codelists for "Psoriasis and dementia: a population-based matched cohort study of adults in England"

## How to run

1.  Open the R console and call `renv::restore()` to install the required R packages.
2.  Provide a `paths.R` file in the `paths/` folder (see [README](paths/README.md))
3.  Call `library(targets)` and `tar_make()` to run the pipeline (if running with dummy data, run `tar_make(dummy_define)` first).

## How to inspect targets

-   Call `tar_read(target)` to retrieve a specified target.
-   Call `tar_visnetwork(targets_only = TRUE)` to visualise the pipeline.

## Working with CPRD Aurum Define & Extract system

No real data is provided in this repository. CPRD Aurum data is provided via a 2-step process: First, the codes from the relevant codelist need to be provided to *define* a cohort of exposed individuals (the files are created as **[...]\_codes_for_define.txt**). Second, a list of patids needs to be provided to *extract* all data for these individuals (the list of patids is created as **patids_for_extract.txt**).

## Typical run time and hardware requirements

With dummy data the pipeline takes approximately 5 minutes to complete on a normal (ca 2024) desktop or laptop computer. With real CPRD Aurum data, the pipeline may take several hours to complete when branches are not run in parallel. Approximately 64 GB of RAM are required.

## Files

| File                                   | Purpose                                                                                                        |
|----------------------------------------|----------------------------------------------------------------------------------------------------------------|
| [\_targets.R](_targets.R)              | Declares the [`targets`](https://docs.ropensci.org/targets) pipeline.                                          |
| [R/](R/)                               | Contains R scripts with functions to be used in the pipeline.                                                  |
| [codelists/](codelists/)               | Contains all codelists.                                                                                        |
| [input/](input/)                       | Contains prednisolone conversion factors.                                                                      |
| [dummy_data/](dummy_data/)             | When the pipeline is run, dummy data will be placed here.                                                      |
| [output/](output/)                     | When the pipeline is run, outputs will be placed here.                                                         |
| [sensitive_output/](sensitive_output/) | When the pipeline is run, sensitive outputs will be placed here (lists for CPRD Aurum define and extract).     |
| [paths/](paths/)                       | To run the pipeline, the `paths.R` file needs to be placed here (see [README](paths/README.md)).               |
| [sensitive_input/](sensitive_input/)   | To run the pipeline, the CPRD Aurum codebrowser and dosage information files need to be placed here.           |
| [renv.lock](renv.lock)                 | The [`renv`](https://rstudio.github.io/renv/articles/renv.html) lock file that specifies all package versions. |

# load libraries
library(fixest)
library(arrow)
library(dplyr)
library(modelsummary)
library(sf)
library(ggplot2)

library(tidyr)
library(stringr)

library(knitr)
library(kableExtra)

library(rprojroot)


source("scripts/10_clean/11_clean_alerts.R")
source("scripts/10_clean/12_clean_burned_area.R")
source("scripts/10_clean/13_clean_chirps.R")
source("scripts/10_clean/14_clean_clouds.R")
source("scripts/10_clean/15_clean_forestloss.R")
source("scripts/10_clean/16_clean_landcover.R")

source("scripts/20_build/21_build_processed.R")
source("scripts/20_build/22_clean_final.R")

source("scripts/30_regprep/30_prep_data_reg.R")

source("scripts/40_models/41_baseline_feols.R")
source("scripts/40_models/iv_nolag.R")
source("scripts/40_models/iv_lag1.R")
source("scripts/40_models/dyn_2sls.R")
source("scripts/40_models/42_iv_lag1_main.R")
source("scripts/40_models/43_dyn_2sls_main.R")
source("scripts/40_models/44_iv_lag1_frontier_strict.R")
source("scripts/40_models/45_dyn_2sls_frontier_strict.R")
source("scripts/40_models/46_dyn_2sls_frontier_loose_med.R")


source("scripts/50_tables/51_table_baseline_feols.R")
source("scripts/50_tables/table_iv_nolag.R")
source("scripts/50_tables/table_iv_lag1.R")
source("scripts/50_tables/table_dyn_2sls.R")
source("scripts/50_tables/52_table_iv_lag1_main.R")
source("scripts/50_tables/53_table_dyn_2sls_main.R")
source("scripts/50_tables/54_table_iv_lag1_frontier_strict.R")
source("scripts/50_tables/55_table_dyn_2sls_frontier_strict.R")
source("scripts/50_tables/56_table_dyn_2sls_frontier_loose_med.R")



source("scripts/21_choropleth_map.Rmd")
source("scripts/22_histograms.R")
source("scripts/23_scatterplots.R")

# scripts/09_clean_final.R

library(tidyverse)
library(arrow)

# 前処理ユーティリティ（write_parquet_safely など）
source("R/utils_io.R")

# In this R script we conduct the data cleaning by limiting years and variables, generating them, and make a new parquet file to enable us to analyse it easily. 

# --------------------------------------------------
# 1. Load parquet data
# --------------------------------------------------
panel_path <- "data/processed/adm2_panel.parquet"

if (!file.exists(panel_path)) {
  stop("adm2_panel.parquet does not exist. Run 08_build_processed.R at first.")
}

panel <- read_parquet(panel_path)

# --------------------------------------------------
# 2. Limiting samples and generating new variables
# --------------------------------------------------
reg_dt <- panel %>%
  # Periods : between 2019 and 2024 
  filter(year >= 2019, year <= 2024) %>%
  
  # 森林がほとんどない ADM2 を除外（閾値は調整可）
  # filter(forest2000_ha > 1000) %>%
  
  # select variables
  select(
    adm1_name, adm2_name, adm2_code, year,
    ha_alerts,
    chirps_mm, chirps_rainy_mm,
    cloud_share,
    burned_ha,
    defor_rate,
    forest2000_ha,
    loss_ha,
    share_barren,
    share_closed_shrub,
    share_cropland,
    share_cropland_natural_mosaic,
    share_deciduous_broadleaf,
    share_deciduous_needle,
    share_evergreen_broadleaf,
    share_evergreen_needle,
    share_grassland,
    share_mixed_forest,
    share_open_shrub,
    share_savanna,
    share_snow_ice,
    share_urban,
    share_water,
    share_wetland,
    share_woody_savanna,
    total_ha
  ) %>%
  # generating log variables
  mutate(
    ln_alerts = log1p(ha_alerts),   # = log(ha_alerts + 1)
    ln_loss   = log1p(loss_ha),     # = log(loss_ha   + 1)
    ln_burned = log1p(burned_ha)    # = log(burned_ha + 1)
  ) 

# --------------------------------------------------
# 3. Exporting parquet data for analysis
# --------------------------------------------------
out_path <- "data/processed/adm2_reg_2019_2024.parquet"
write_parquet_safely(reg_dt, out_path)

# --------------------------------------------------
# 4. Checkers for exporting
# --------------------------------------------------
cat("\nSaved analysis dataset to:", out_path, "\n")
cat("Number of rows:", nrow(reg_dt), "\n")
cat("Number of columns:", ncol(reg_dt), "\n\n")

print(summary(reg_dt$ha_alerts))

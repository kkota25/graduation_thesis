# scripts/30_perp_data_reg.R
# --------------------------------------------------
# Build analysis-ready datasets from data/processed/adm2_panel.parquet
#  - reg_dt  : main analysis panel (ADM2 x year), with logs
#  - reg_dyn : dynamic panel with lags (drops first year per ADM2)
#  - reg_dyn_nz : reg_dyn further dropping ln_alerts == 0 (as in Rmd)
#  - sample selections:
#      * main islands
#      * forest share >= 0.02
#      * frontier (loose/strict) + robustness (median forest share)
# --------------------------------------------------

suppressPackageStartupMessages({
  library(dplyr)
  library(arrow)
  library(tibble)
  library(rprojroot)
})

# ---------- project paths ----------
proj_root <- rprojroot::find_rstudio_root_file()
path_in   <- file.path(proj_root, "data", "processed", "adm2_panel.parquet")
dir_proc  <- file.path(proj_root, "data", "processed")
dir_meta  <- file.path(proj_root, "outputs", "metadata")

# utilities (write_parquet_safely)
source(file.path(proj_root, "R", "utils_io.R"))

# ---------- parameters ----------
YEAR_MIN <- 2019
YEAR_MAX <- 2024

# ---------- checks ----------
if (!file.exists(path_in)) {
  stop("adm2_panel.parquet does not exist. Run scripts/20_build/21_build_processed.R first: ", path_in)
}

# ---------- load ----------
panel <- arrow::read_parquet(path_in)

# helper: assert columns exist
assert_has_cols <- function(df, cols, df_name = "data") {
  miss <- setdiff(cols, names(df))
  if (length(miss) > 0) {
    stop(df_name, " is missing required columns: ", paste(miss, collapse = ", "))
  }
  invisible(TRUE)
}

# --------------------------------------------------
# 1) reg_dt: select variables + generate logs
# --------------------------------------------------
req_cols_dt <- c(
  "adm1_name", "adm2_name", "adm2_code", "year",
  "ha_alerts", "chirps_mm", "cloud_share", "burned_ha",
  "defor_rate", "forest2000_ha", "loss_ha", "total_ha"
)
assert_has_cols(panel, req_cols_dt, "adm2_panel")

# landcover shares (optional: keep if exist)
lc_share_cols <- c(
  "share_barren", "share_closed_shrub", "share_cropland",
  "share_cropland_natural_mosaic", "share_deciduous_broadleaf",
  "share_deciduous_needle", "share_evergreen_broadleaf",
  "share_evergreen_needle", "share_grassland", "share_mixed_forest",
  "share_open_shrub", "share_savanna", "share_snow_ice",
  "share_urban", "share_water", "share_wetland",
  "share_woody_savanna"
)
lc_share_cols <- intersect(lc_share_cols, names(panel))

reg_dt <- panel %>%
  filter(year >= YEAR_MIN, year <= YEAR_MAX) %>%
  select(
    any_of(c(
      "adm1_name", "adm2_name", "adm2_code", "year",
      "ha_alerts",
      "chirps_mm",
      # chirps_rainy_mm は 09_clean_final にあったので、存在すれば保持
      "chirps_rainy_mm",
      "cloud_share",
      "burned_ha",
      "defor_rate",
      "forest2000_ha",
      "loss_ha",
      lc_share_cols,
      "total_ha"
    ))
  ) %>%
  mutate(
    ln_alerts = log1p(ha_alerts),
    ln_loss   = log1p(loss_ha),
    ln_burned = log1p(burned_ha)
  )

out_reg_dt <- file.path(dir_proc, "adm2_reg_2019_2024.parquet")
dir.create(dirname(out_reg_dt), recursive = TRUE, showWarnings = FALSE)
write_parquet_safely(reg_dt, out_reg_dt)

message("Saved: ", out_reg_dt, "  (n=", nrow(reg_dt), ", p=", ncol(reg_dt), ")")

# --------------------------------------------------
# 2) reg_dyn: create lags (defor_rate_l1, ln_alerts_l1, cloud_share_l1)
#    - This mirrors your Rmd dyn_data chunk
# --------------------------------------------------
req_cols_dyn <- c("adm2_code", "year", "defor_rate", "ln_alerts", "cloud_share")
assert_has_cols(reg_dt, req_cols_dyn, "reg_dt")

reg_dyn <- reg_dt %>%
  arrange(adm2_code, year) %>%
  group_by(adm2_code) %>%
  mutate(
    defor_rate_l1  = dplyr::lag(defor_rate),
    ln_alerts_l1   = dplyr::lag(ln_alerts),
    cloud_share_l1 = dplyr::lag(cloud_share)
  ) %>%
  ungroup() %>%
  filter(
    !is.na(defor_rate_l1),
    !is.na(ln_alerts_l1),
    !is.na(cloud_share_l1)
  )

out_reg_dyn <- file.path(dir_proc, "reg_dyn.parquet")
write_parquet_safely(reg_dyn, out_reg_dyn)
message("Saved: ", out_reg_dyn, "  (n=", nrow(reg_dyn), ")")

# reg_dyn_nz: drop ln_alerts missing / zero (as in your Rmd)
reg_dyn_nz <- reg_dyn %>%
  filter(!is.na(ln_alerts), ln_alerts != 0)

out_reg_dyn_nz <- file.path(dir_proc, "reg_dyn_nz.parquet")
write_parquet_safely(reg_dyn_nz, out_reg_dyn_nz)
message("Saved: ", out_reg_dyn_nz, "  (n=", nrow(reg_dyn_nz), ")")


# reg_lag: (only ln_alerts_l1 and cloud_share_l1) as in your make_lags chunk
# made variables with a lag

reg_lag <- reg_dt %>%
  arrange(adm2_code, year) %>%
  group_by(adm2_code) %>%
  mutate(
    ln_alerts_l1   = dplyr::lag(ln_alerts,   n = 1),
    cloud_share_l1 = dplyr::lag(cloud_share, n = 1)
  ) %>%
  ungroup() %>%
  filter(!is.na(ln_alerts_l1), !is.na(cloud_share_l1))

out_reg_lag <- file.path(dir_proc, "reg_lag.parquet")
write_parquet_safely(reg_lag, out_reg_lag)
message("Saved: ", out_reg_lag, "  (n=", nrow(reg_lag), ")")

# --------------------------------------------------
# 3) Sample selection: main forest islands (drop Java, Bali, Nusa Tenggara, Maluku, Riau Islands)
#    - Mirrors your Rmd "extract islands"
# --------------------------------------------------
assert_has_cols(reg_dyn, c("adm1_name"), "reg_dyn")

drop_adm1 <- c(
  # Java
  "Banten",
  "Dki Jakarta",
  "Jawa Barat",
  "Jawa Tengah",
  "Jawa Timur",
  "Daerah Istimewa Yogyakarta",
  # Bali + Nusa Tenggara
  "Bali",
  "Nusatenggara Barat",
  "Nusatenggara Timur",
  # Maluku
  "Maluku",
  "Maluku Utara",
  # Riau Islands
  "Kepulauan-riau"
)

reg_dyn_main_islands <- reg_dyn %>%
  filter(!adm1_name %in% drop_adm1)

out_main_islands <- file.path(dir_proc, "reg_dyn_main_islands.parquet")
write_parquet_safely(reg_dyn_main_islands, out_main_islands)
message("Saved: ", out_main_islands, "  (n=", nrow(reg_dyn_main_islands), ")")

# --------------------------------------------------
# 4) Forest share in 2000 and thresholds (>= 0.02), median robustness
# --------------------------------------------------
assert_has_cols(reg_dyn_main_islands, c("forest2000_ha", "total_ha"), "reg_dyn_main_islands")

reg_dyn_main_islands <- reg_dyn_main_islands %>%
  mutate(share_forest2000 = forest2000_ha / total_ha)

# >= 2%
reg_dyn_forest02 <- reg_dyn_main_islands %>%
  filter(share_forest2000 >= 0.02)

out_forest02 <- file.path(dir_proc, "reg_dyn_forest02.parquet")
write_parquet_safely(reg_dyn_forest02, out_forest02)
message("Saved: ", out_forest02, "  (n=", nrow(reg_dyn_forest02), ")")

# median threshold
med_share_forest2000 <- median(reg_dyn_main_islands$share_forest2000, na.rm = TRUE)

reg_dyn_forest_median <- reg_dyn_main_islands %>%
  filter(share_forest2000 >= med_share_forest2000)

out_forest_median <- file.path(dir_proc, "reg_dyn_forest_median.parquet")
write_parquet_safely(reg_dyn_forest_median, out_forest_median)
message("Saved: ", out_forest_median, "  (n=", nrow(reg_dyn_forest_median), ")")

# --------------------------------------------------
# 5) Frontier definitions (loose/strict) based on max loss and max defor across years
#    - Mirrors your Rmd frontier blocks
#    NOTE: reg_dyn_* already drops first year per ADM2 (lag construction).
# --------------------------------------------------
assert_has_cols(reg_dyn_forest02, c("loss_ha", "defor_rate", "adm2_code", "adm1_name", "adm2_name"), "reg_dyn_forest02")

dyn_frontier_stats <- reg_dyn_forest02 %>%
  group_by(adm2_code, adm1_name, adm2_name) %>%
  summarise(
    n_years      = n(),
    mean_loss_ha = mean(loss_ha, na.rm = TRUE),
    max_loss_ha  = max(loss_ha,  na.rm = TRUE),
    mean_defor   = mean(defor_rate, na.rm = TRUE),
    max_defor    = max(defor_rate,  na.rm = TRUE),
    .groups = "drop"
  )

q_max_loss  <- quantile(dyn_frontier_stats$max_loss_ha, probs = c(0.25, 0.5), na.rm = TRUE)
q_max_defor <- quantile(dyn_frontier_stats$max_defor,   probs = c(0.25, 0.5), na.rm = TRUE)

dyn_frontier_stats <- dyn_frontier_stats %>%
  mutate(
    is_frontier_loose  = (max_loss_ha >= q_max_loss[1]) | (max_defor >= q_max_defor[1]),
    is_frontier_strict = (max_loss_ha >= q_max_loss[2]) | (max_defor >= q_max_defor[2])
  )

# attach flags back
reg_dyn_frontier <- reg_dyn_forest02 %>%
  left_join(
    dyn_frontier_stats %>% select(adm2_code, is_frontier_loose, is_frontier_strict),
    by = "adm2_code"
  )

reg_dyn_frontier_loose <- reg_dyn_frontier %>% filter(is_frontier_loose)
reg_dyn_frontier_strict <- reg_dyn_frontier %>% filter(is_frontier_strict)

out_frontier_loose  <- file.path(dir_proc, "reg_dyn_frontier_loose.parquet")
out_frontier_strict <- file.path(dir_proc, "reg_dyn_frontier_strict.parquet")
write_parquet_safely(reg_dyn_frontier_loose,  out_frontier_loose)
write_parquet_safely(reg_dyn_frontier_strict, out_frontier_strict)
message("Saved: ", out_frontier_loose,  "  (n=", nrow(reg_dyn_frontier_loose), ")")
message("Saved: ", out_frontier_strict, "  (n=", nrow(reg_dyn_frontier_strict), ")")

# frontier + median forest share robustness
reg_dyn_frontier_loose_med <- reg_dyn_frontier %>%
  filter(is_frontier_loose, share_forest2000 >= med_share_forest2000)

reg_dyn_frontier_strict_med <- reg_dyn_frontier %>%
  filter(is_frontier_strict, share_forest2000 >= med_share_forest2000)

out_frontier_loose_med  <- file.path(dir_proc, "reg_dyn_frontier_loose_med.parquet")
out_frontier_strict_med <- file.path(dir_proc, "reg_dyn_frontier_strict_med.parquet")
write_parquet_safely(reg_dyn_frontier_loose_med,  out_frontier_loose_med)
write_parquet_safely(reg_dyn_frontier_strict_med, out_frontier_strict_med)
message("Saved: ", out_frontier_loose_med,  "  (n=", nrow(reg_dyn_frontier_loose_med), ")")
message("Saved: ", out_frontier_strict_med, "  (n=", nrow(reg_dyn_frontier_strict_med), ")")

# save frontier stats + thresholds (metadata)
dir.create(dir_meta, recursive = TRUE, showWarnings = FALSE)

out_frontier_stats <- file.path(dir_proc, "dyn_frontier_stats.parquet")
write_parquet_safely(dyn_frontier_stats, out_frontier_stats)

thresholds <- tibble(
  year_min = YEAR_MIN,
  year_max = YEAR_MAX,
  med_share_forest2000 = med_share_forest2000,
  q25_max_loss_ha = unname(q_max_loss[1]),
  q50_max_loss_ha = unname(q_max_loss[2]),
  q25_max_defor   = unname(q_max_defor[1]),
  q50_max_defor   = unname(q_max_defor[2])
)

out_thresholds <- file.path(dir_meta, "frontier_thresholds.csv")
readr::write_csv(thresholds, out_thresholds)

message("Saved: ", out_frontier_stats)
message("Saved: ", out_thresholds)

message("=== 10_perp_data_reg.R completed successfully ===")

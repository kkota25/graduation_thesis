# scripts/40_models/iv_lag1.R

# ---- paths ----
proj_root <- rprojroot::find_rstudio_root_file()



model_dir <- file.path(proj_root, "outputs", "models")
model_rds <- file.path(model_dir, "model_iv_lag1.rds")

dir.create(model_dir, recursive = TRUE, showWarnings = FALSE)


# ---- estimate: first stage (lag) ----
fs_ln_alerts_l1 <- feols(
  ln_alerts_l1 ~ cloud_share_l1 + chirps_mm + burned_ha |
    adm2_code + year,
  data    = reg_lag,
  cluster = ~adm2_code
)

# ---- estimate: 2SLS (lag) ----
iv_defor_l1 <- feols(
  defor_rate ~ chirps_mm + burned_ha |      # controls (t)
    adm2_code + year |                      # FE
    ln_alerts_l1 ~ cloud_share_l1,          # endogenous ~ IV (lag1)
  data    = reg_lag,
  cluster = ~adm2_code
)

# ---- save ----
saveRDS(
  list(
    fs_ln_alerts_l1 = fs_ln_alerts_l1,
    iv_defor_l1     = iv_defor_l1,
    meta = list(
      reg_path   = reg_path,
      created_at = Sys.time()
    )
  ),
  model_rds
)

# ---- console check ----
cat("Saved model RDS:", model_rds, "\n\n")
cat("First stage (lag1) summary:\n")
print(summary(fs_ln_alerts_l1))
cat("\n2SLS (lag1) summary:\n")
print(summary(iv_defor_l1))

# scripts/40_models/dyn_2sls.R

# ---- paths ----
proj_root <- rprojroot::find_rstudio_root_file()

model_dir <- file.path(proj_root, "outputs", "models")
model_rds <- file.path(model_dir, "model_dyn_2sls.rds")
dir.create(model_dir, recursive = TRUE, showWarnings = FALSE)


# ---- estimate dynamic 2SLS ----
dyn_iv <- feols(
  defor_rate ~ defor_rate_l1 + chirps_mm + burned_ha |
    adm2_code + year |
    ln_alerts_l1 ~ cloud_share_l1,
  data    = reg_dyn,
  cluster = ~adm2_code
)

# ---- save ----
saveRDS(
  list(
    dyn_iv = dyn_iv,
    meta = list(
      reg_path   = reg_path,
      created_at = Sys.time()
    )
  ),
  model_rds
)

# ---- console check ----
cat("Saved model RDS:", model_rds, "\n")
cat("Input reg_dyn parquet:", reg_path, "\n\n")

cat("Stage 1 summary:\n")
print(summary(dyn_iv, stage = 1))
cat("\nStage 2 summary:\n")
print(summary(dyn_iv, stage = 2))

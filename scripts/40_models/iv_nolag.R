# scripts/40_models/iv_nolag.R


# ---- paths ----
proj_root <- rprojroot::find_rstudio_root_file()

reg_path  <- file.path(proj_root, "data", "processed", "adm2_reg_2019_2024.parquet")
model_dir <- file.path(proj_root, "outputs", "models")
model_rds <- file.path(model_dir, "model_iv_nolag.rds")

dir.create(model_dir, recursive = TRUE, showWarnings = FALSE)

# ---- load data ----
if (!file.exists(reg_path)) {
  stop(
    "reg parquet not found: ", reg_path,
    "\n先に scripts/40_regprep/（regデータ作成）を実行してください。"
  )
}
reg_dt <- arrow::read_parquet(reg_path)

# ---- estimate: first stage (separate) ----
fs_ln_alerts <- feols(
  ln_alerts ~ cloud_share + chirps_mm + burned_ha |
    adm2_code + year,
  data    = reg_dt,
  cluster = ~adm2_code
)

# ---- estimate: 2SLS (fixest IV syntax) ----
iv_defor <- feols(
  defor_rate ~ chirps_mm + burned_ha |   # exogenous controls
    adm2_code + year |                   # FE
    ln_alerts ~ cloud_share,             # endogenous ~ IV
  data    = reg_dt,
  cluster = ~adm2_code
)

# ---- save ----
saveRDS(
  list(
    fs_ln_alerts = fs_ln_alerts,
    iv_defor     = iv_defor,
    meta = list(
      reg_path   = reg_path,
      created_at = Sys.time()
    )
  ),
  model_rds
)

# ---- console check ----
cat("Saved model RDS:", model_rds, "\n\n")
cat("First stage summary:\n")
print(summary(fs_ln_alerts))
cat("\n2SLS summary:\n")
print(summary(iv_defor))

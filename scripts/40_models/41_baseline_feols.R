# scripts/40_models/41_baseline_feols.R


# ---- paths ----
proj_root <- rprojroot::find_rstudio_root_file()

reg_path  <- file.path(proj_root, "data", "processed", "adm2_reg_2019_2024.parquet")
model_dir <- file.path(proj_root, "outputs", "models")
model_rds <- file.path(model_dir, "model_baseline_feols.rds")

dir.create(model_dir, recursive = TRUE, showWarnings = FALSE)

# ---- load data ----
if (!file.exists(reg_path)) {
  stop("reg parquet not found: ", reg_path,
       "\n先に scripts/40_regprep/（regデータ作成）を実行してください。")
}
reg_dt <- arrow::read_parquet(reg_path)

# ---- estimate ----
ols_defor <- feols(
  defor_rate ~ ln_alerts + chirps_mm + burned_ha |
    adm2_code + year,
  data    = reg_dt,
  cluster = ~adm2_code
)

# ---- save model ----
saveRDS(
  list(
    ols_defor = ols_defor,
    meta = list(
      reg_path = reg_path,
      created_at = Sys.time()
    )
  ),
  model_rds
)

# ---- console check ----
print(paste0("Saved model RDS: ", model_rds))
print(summary(ols_defor))

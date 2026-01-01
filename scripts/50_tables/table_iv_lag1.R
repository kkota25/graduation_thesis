# scripts/50_tables/table_iv_lag1.R



# ---- paths ----
proj_root <- rprojroot::find_rstudio_root_file()

model_rds <- file.path(proj_root, "outputs", "models", "model_iv_lag1.rds")
table_dir <- file.path(proj_root, "outputs", "tables")
out_tex   <- file.path(table_dir, "table_iv_lag1.tex")

dir.create(table_dir, recursive = TRUE, showWarnings = FALSE)

# ---- load ----
if (!file.exists(model_rds)) {
  stop(
    "model RDS not found: ", model_rds,
    "\n先に scripts/40_models/iv_lag1.R を実行してください。"
  )
}

obj <- readRDS(model_rds)
fs_ln_alerts_l1 <- obj$fs_ln_alerts_l1
iv_defor_l1     <- obj$iv_defor_l1

# ---- export tex ----
modelsummary(
  list(
    "First stage (lag1): ln(alerts+1)_{t-1}" = fs_ln_alerts_l1,
    "Second stage (lag1): defor_rate_t"      = iv_defor_l1
  ),
  output    = out_tex,
  stars     = TRUE,
  statistic = "std.error",
  gof_omit  = "IC|Log|Adj|Within|FE"
)

cat("Wrote TeX table:", out_tex, "\n")
cat("file.exists(out_tex) =", file.exists(out_tex), "\n")

# scripts/50_tables/52_table_iv_lag1_main.R



# ---- paths ----
proj_root <- rprojroot::find_rstudio_root_file()

model_rds <- file.path(proj_root, "outputs", "models", "model_iv_lag1_main.rds")
table_dir <- file.path(proj_root, "outputs", "tables")
out_tex   <- file.path(table_dir, "52_table_iv_lag1_main.tex")

dir.create(table_dir, recursive = TRUE, showWarnings = FALSE)

# ---- load models ----
if (!file.exists(model_rds)) {
  stop(
    "model RDS not found: ", model_rds,
    "\n先に scripts/40_models/42_iv_lag1_main.R を実行してください。"
  )
}

obj <- readRDS(model_rds)
fs_ln_alerts_l1 <- obj$fs_ln_alerts_l1
iv_defor_l1     <- obj$iv_defor_l1

# ---- export TeX table ----
modelsummary(
  list(
    "First stage (lag1): ln_alerts_l1" = fs_ln_alerts_l1,
    "Second stage (lag1): defor_rate"  = iv_defor_l1
  ),
  output    = out_tex,
  stars     = TRUE,
  statistic = "std.error",
  gof_omit  = "IC|Log|Adj|Within|FE"
)

cat("Wrote TeX table:", out_tex, "\n")
cat("file.exists(out_tex) =", file.exists(out_tex), "\n")

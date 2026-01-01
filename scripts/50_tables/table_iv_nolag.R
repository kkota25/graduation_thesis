# scripts/50_tables/table_iv_nolag.R

# ---- packages ----
library(modelsummary)
library(rprojroot)

# ---- paths ----
proj_root <- rprojroot::find_rstudio_root_file()

model_rds <- file.path(proj_root, "outputs", "models", "model_iv_nolag.rds")
table_dir <- file.path(proj_root, "outputs", "tables")
out_tex   <- file.path(table_dir, "table_iv_nolag.tex")

dir.create(table_dir, recursive = TRUE, showWarnings = FALSE)

# ---- load ----
if (!file.exists(model_rds)) {
  stop(
    "model RDS not found: ", model_rds,
    "\n先に scripts/40_models/iv_nolag.R を実行してください。"
  )
}

obj <- readRDS(model_rds)
fs_ln_alerts <- obj$fs_ln_alerts
iv_defor     <- obj$iv_defor

# ---- export tex ----
modelsummary(
  list(
    "First stage: ln(alerts+1)" = fs_ln_alerts,
    "Second stage: defor_rate"  = iv_defor
  ),
  output    = out_tex,
  stars     = TRUE,
  statistic = "std.error",
  gof_omit  = "IC|Log|Adj|Within|FE"
)

cat("Wrote TeX table:", out_tex, "\n")
cat("file.exists(out_tex) =", file.exists(out_tex), "\n")

# scripts/50_tables/51_table_baseline_feols.R



# ---- paths ----
proj_root <- rprojroot::find_rstudio_root_file()

model_rds <- file.path(proj_root, "outputs", "models", "model_baseline_feols.rds")
table_dir <- file.path(proj_root, "outputs", "tables")
out_tex   <- file.path(table_dir, "table_baseline_feols.tex")

dir.create(table_dir, recursive = TRUE, showWarnings = FALSE)

# ---- load model ----
if (!file.exists(model_rds)) {
  stop("model RDS not found: ", model_rds,
       "\n先に scripts/40_models/41_baseline_feols.R を実行してください。")
}
obj <- readRDS(model_rds)
ols_defor <- obj$ols_defor

# ---- export table ----
modelsummary(
  list("FE-OLS (TWFE)" = ols_defor),
  output    = out_tex,
  stars     = TRUE,
  statistic = "std.error",
  gof_omit  = "IC|Log|Adj|Within|FE"
)

print(paste0("Wrote TeX table: ", out_tex))

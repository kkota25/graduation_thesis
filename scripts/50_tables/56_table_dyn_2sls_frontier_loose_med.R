# scripts/50_tables/56_table_dyn_2sls_frontier_loose_med.R

# ---- paths ----
proj_root <- rprojroot::find_rstudio_root_file()

model_rds <- file.path(proj_root, "outputs", "models", "model_dyn_2sls_frontier_loose_med.rds")
table_dir <- file.path(proj_root, "outputs", "tables")
out_tex   <- file.path(table_dir, "56_dyn_2sls_frontier_loose_med.tex")

dir.create(table_dir, recursive = TRUE, showWarnings = FALSE)

# ---- load model ----
if (!file.exists(model_rds)) {
  stop(
    "model RDS not found: ", model_rds,
    "\n先に scripts/40_models/46_dyn_2sls_frontier_loose_med.R を実行してください。"
  )
}

obj <- readRDS(model_rds)
dyn_iv <- obj$dyn_iv

# ---- build TeX table (stage 1 -> stage 2) ----
tex_both <- etable(
  summary(dyn_iv, stage = 1:2),
  tex = TRUE,
  fitstat = ~ . + ivfall + ivwaldall.p
)

writeLines(tex_both, out_tex)

cat("Wrote TeX table:", out_tex, "\n")
cat("file.exists(out_tex) =", file.exists(out_tex), "\n")

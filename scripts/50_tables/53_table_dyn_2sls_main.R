# scripts/50_tables/53_table_dyn_2sls_main.R

# ---- paths ----
proj_root <- rprojroot::find_rstudio_root_file()

model_rds <- file.path(proj_root, "outputs", "models", "model_dyn_2sls_main.rds")
table_dir <- file.path(proj_root, "outputs", "tables")
out_tex   <- file.path(table_dir, "53_table_dyn_2sls_main.tex")

dir.create(table_dir, recursive = TRUE, showWarnings = FALSE)

# ---- load model ----
if (!file.exists(model_rds)) {
  stop(
    "model RDS not found: ", model_rds,
    "\n先に scripts/40_models/32_dyn_2sls_frontier_loose.R を実行してください。"
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

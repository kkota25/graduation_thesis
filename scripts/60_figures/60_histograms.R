# scripts/60_figures/60_histograms.R
# ------------------------------------------------------------
# 目的: 回帰用パネル（adm2_reg_2019_2024.parquet）から主要変数のヒストグラムを一括出力
# 入力: data/processed/adm2_reg_2019_2024.parquet
# 出力:
#   - outputs/figures/hist/<var>_hist.png
#   - outputs/figures/hist/histograms_all.pdf
# 依存: arrow, dplyr, ggplot2, fs, gridExtra
# ------------------------------------------------------------

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(ggplot2)
  library(fs)
  library(gridExtra)
})

# -----------------------
# pass
# -----------------------
in_file  <- "data/processed/adm2_reg_2019_2024.parquet"
out_dir  <- "outputs/figures/hist"
dir_create(out_dir, recurse = TRUE)

# -----------------------
# data
# -----------------------
if (!file_exists(in_file)) {
  stop(paste0("入力ファイルが見つかりません: ", in_file))
}
reg_dt <- read_parquet(in_file) |> as.data.frame()

# -----------------------
# function for making histgrams
# -----------------------
make_hist <- function(df, var, bins = 30, title = NULL, xlab = NULL, filter_expr = NULL) {
  if (!var %in% names(df)) return(NULL)
  
  d <- df
  if (!is.null(filter_expr)) {
    d <- d |> dplyr::filter(!!rlang::parse_expr(filter_expr))
  }
  
  # 有効値がほぼ無い場合はスキップ
  x <- d[[var]]
  if (all(is.na(x)) || sum(is.finite(x), na.rm = TRUE) < 10) return(NULL)
  
  if (is.null(title)) title <- paste0("Histogram of ", var)
  if (is.null(xlab))  xlab  <- var
  
  ggplot(d, aes(x = .data[[var]])) +
    geom_histogram(bins = bins) +
    labs(title = title, x = xlab, y = "Count") +
    theme_minimal()
}

# -----------------------
# 対象変数（hotspot/ntl は入れない）
# ※ データに無い列は自動でスキップ
# -----------------------
specs <- list(
  list(var = "cloud_share",  bins = 30, title = "Histogram of cloud_share",  xlab = "cloud_share"),
  list(var = "chirps_mm",    bins = 30, title = "Histogram of chirps_mm",    xlab = "chirps_mm"),
  list(var = "burned_ha",    bins = 30, title = "Histogram of burned_ha",    xlab = "burned_ha"),
  list(var = "defor_rate",   bins = 30, title = "Histogram of defor_rate",   xlab = "defor_rate"),
  list(var = "ha_alerts",    bins = 30, title = "Histogram of ha_alerts",    xlab = "ha_alerts"),
  list(var = "ln_alerts",    bins = 30, title = "Histogram of ln_alerts (ha_alerts > 0)", xlab = "ln(ha_alerts + 1)", filter_expr = "ha_alerts > 0"),
  list(var = "ln_burned",    bins = 30, title = "Histogram of ln_burned (burned_ha > 0)", xlab = "ln(burned_ha + 1)", filter_expr = "burned_ha > 0"),
  list(var = "ln_loss",      bins = 30, title = "Histogram of ln_loss",      xlab = "ln(loss + 1)"),
  list(var = "forest2000_ha",bins = 30, title = "Histogram of forest2000_ha",xlab = "forest2000_ha"),
  list(var = "total_ha",     bins = 30, title = "Histogram of total_ha",     xlab = "total_ha")
)

plots <- list()

for (sp in specs) {
  p <- make_hist(
    df         = reg_dt,
    var        = sp$var,
    bins       = sp$bins %||% 30,
    title      = sp$title %||% NULL,
    xlab       = sp$xlab  %||% NULL,
    filter_expr= sp$filter_expr %||% NULL
  )
  if (is.null(p)) next
  
  png_path <- file.path(out_dir, paste0(sp$var, "_hist.png"))
  ggsave(filename = png_path, plot = p, width = 7, height = 5, dpi = 300)
  
  plots[[sp$var]] <- p
}

# 全部まとめPDF（作れたものだけ）
if (length(plots) > 0) {
  pdf_path <- file.path(out_dir, "histograms_all.pdf")
  pdf(pdf_path, width = 7, height = 5)
  for (nm in names(plots)) {
    print(plots[[nm]])
  }
  dev.off()
} else {
  message("作成できるヒストグラムがありませんでした（列が存在しない/有効値不足など）。")
}

message("DONE: histograms -> ", out_dir)

# scripts/60_figures/62_scatterplots.R
# ------------------------------------------------------------
# 目的: 主要ペアの散布図を保存（外れ値上位1%トリムは従属側のみ）
# 入力: data/processed/adm2_reg_2019_2024.parquet
# 出力: outputs/figures/scatter/<name>.png
# 依存: arrow, dplyr, ggplot2, fs
# ------------------------------------------------------------

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(ggplot2)
  library(fs)
})

in_file <- "data/processed/adm2_reg_2019_2024.parquet"
out_dir <- "outputs/figures/scatter"
dir_create(out_dir, recurse = TRUE)

if (!file_exists(in_file)) {
  stop(paste0("入力ファイルが見つかりません: ", in_file))
}
reg_dt <- read_parquet(in_file) |> as.data.frame()

trim_top1 <- function(df, var) {
  if (!var %in% names(df)) return(df)
  up <- quantile(df[[var]], 0.99, na.rm = TRUE)
  df |> filter(.data[[var]] <= up)
}

save_scatter <- function(df, x, y, name, xlab = NULL, ylab = NULL, trim_y = TRUE) {
  if (!x %in% names(df) || !y %in% names(df)) {
    message("SKIP (missing columns): ", name, " [", x, ", ", y, "]")
    return(invisible(NULL))
  }
  d <- df
  if (trim_y) d <- trim_top1(d, y)
  
  # 有効値不足はスキップ
  ok <- is.finite(d[[x]]) & is.finite(d[[y]])
  if (sum(ok, na.rm = TRUE) < 50) {
    message("SKIP (too few valid obs): ", name)
    return(invisible(NULL))
  }
  
  p <- ggplot(d, aes(x = .data[[x]], y = .data[[y]])) +
    geom_point(alpha = 0.25) +
    geom_smooth(method = "lm", se = FALSE) +
    theme_minimal() +
    labs(
      title = name,
      x = xlab %||% x,
      y = ylab %||% y
    )
  
  out_path <- file.path(out_dir, paste0(name, ".png"))
  ggsave(out_path, p, width = 7, height = 5, dpi = 300)
  invisible(p)
}

# ------------------------------------------------------------
# 描きたい散布図セット（hotspot/ntl なし）
# ------------------------------------------------------------
pairs <- list(
  list(x="ln_alerts",   y="defor_rate",  name="01_ln_alerts_vs_defor_rate", xlab="ln(alerts + 1)", ylab="deforestation rate", trim_y=TRUE),
  list(x="ln_alerts",   y="ln_loss",     name="02_ln_alerts_vs_ln_loss",    xlab="ln(alerts + 1)", ylab="ln(loss + 1)",       trim_y=TRUE),
  list(x="cloud_share", y="ln_alerts",   name="03_cloud_share_vs_ln_alerts",xlab="cloud share",    ylab="ln(alerts + 1)",     trim_y=TRUE),
  list(x="cloud_share", y="defor_rate",  name="04_cloud_share_vs_defor_rate",xlab="cloud share",   ylab="deforestation rate", trim_y=TRUE),
  list(x="cloud_share", y="ln_loss",     name="05_cloud_share_vs_ln_loss",  xlab="cloud share",    ylab="ln(loss + 1)",       trim_y=TRUE),
  list(x="cloud_share", y="chirps_mm",   name="06_cloud_share_vs_chirps_mm",xlab="cloud share",    ylab="CHIRPS (mm)",        trim_y=TRUE)
)

for (pp in pairs) {
  save_scatter(
    df     = reg_dt,
    x      = pp$x,
    y      = pp$y,
    name   = pp$name,
    xlab   = pp$xlab,
    ylab   = pp$ylab,
    trim_y = pp$trim_y
  )
}

message("DONE: scatterplots -> ", out_dir)

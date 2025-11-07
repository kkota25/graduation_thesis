# scripts/07_scatterplots.R
# ------------------------------------------------------------
# 目的: 回帰前に主要説明変数と目的変数の散布図を自動生成
# 入力: data/processed/adm2_panel.parquet
# 出力:
#   - outputs/figures/scatter/<y>_vs_<x>.png（通常とlog1pの2種）
#   - reports/outputs/figures/scatter_all.pdf（まとめ）
# 依存: data.table, arrow, here, ggplot2, fs
# ------------------------------------------------------------

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
  library(here)
  library(ggplot2)
  library(fs)
})

# ---- y 候補の自動検出 ----
.find_y <- function(DT){
  cand <- c("defor_rate","loss_rate","loss_share",
            "loss_ha","forest_loss_ha","treecover_loss_ha")
  y <- intersect(cand, names(DT))
  if (length(y)) y[[1]] else NA_character_
}

# ---- x 候補の抽出（存在するものだけ採用）----
.find_xs <- function(DT){
  groups <- list(
    cloud   = c("cloud_share","cloud_prob","cloud"),
    rain    = c("chirps_mm","rain_mm","precip","rain"),
    fire    = c("ln_burn","burned_ha","burn_ha"),
    ntl     = c("viirs_ntl","ntl"),
    clear   = c("clear_share","forest_cover_share"),
    firms   = c("fire_count","firms_cnt","frp")
  )
  xs <- character()
  for (g in groups) {
    hit <- intersect(g, names(DT))
    if (length(hit)) xs <- c(xs, hit[[1]])
  }
  # 数値列のみ
  xs[vapply(xs, function(v) is.numeric(DT[[v]]), logical(1))]
}

# ---- 1枚の散布図を作成して保存。ggplotオブジェクトを返す ----
.save_scatter <- function(DT, x, y, out_png, log1p_xy = FALSE, bins = 60){
  # 常に data.frame に落とす（ベクトル化事故を防止）
  pdat <- data.frame(x = DT[[x]], y = DT[[y]])
  keep <- is.finite(pdat$x) & is.finite(pdat$y)
  pdat <- pdat[keep, , drop = FALSE]
  if (!nrow(pdat)) return(FALSE)
  
  if (log1p_xy) {
    pdat$x <- log1p(pdat$x)
    pdat$y <- log1p(pdat$y)
  }
  
  g <- ggplot(pdat, aes(x, y)) +
    geom_bin2d(bins = bins) +
    geom_smooth(method = "lm", se = FALSE, linewidth = 0.6) +
    labs(
      title = sprintf("%s vs %s%s", y, x, if (log1p_xy) " (log1p)" else ""),
      x = x, y = y
    ) +
    theme_minimal(base_size = 12) +
    theme(panel.grid.minor = element_blank())
  
  ggsave(out_png, g, width = 8.5, height = 6.0, dpi = 150)
  g
}

# ---- メイン ----
run_scatterplots <- function(panel_path,
                             y_var     = NULL,
                             out_dir   = here("outputs","figures","scatter"),
                             make_pdf  = TRUE,
                             pdf_path  = here("reports","outputs","figures","scatter_all.pdf"),
                             bins      = 60){
  
  if (!file.exists(panel_path)) stop("パネルファイルが見つかりません: ", panel_path)
  fs::dir_create(out_dir, recurse = TRUE)
  fs::dir_create(fs::path_dir(pdf_path), recurse = TRUE)
  
  DT <- as.data.table(arrow::read_parquet(panel_path))
  
  # y の決定
  y_auto <- .find_y(DT)
  y <- if (is.null(y_var)) y_auto else y_var
  if (!length(y) || is.na(y) || !y %in% names(DT))
    stop("目的変数 y が見つかりません。候補例: defor_rate, loss_rate, loss_share, loss_ha, forest_loss_ha, treecover_loss_ha")
  
  # x 候補
  xs <- .find_xs(DT)
  if (!length(xs)) stop("散布図の説明変数候補が見つかりません。")
  
  # PDF まとめ開始
  if (isTRUE(make_pdf)) {
    grDevices::pdf(pdf_path, width = 8.5, height = 6.0, onefile = TRUE, paper = "special")
    on.exit(try(grDevices::dev.off(), silent = TRUE), add = TRUE)
  }
  
  # 各 x と y のペアで通常版と log1p 版を出力
  for (x in xs) {
    # 通常スケール
    fn_png1 <- file.path(out_dir, sprintf("%s_vs_%s.png", y, x))
    g1 <- .save_scatter(DT, x, y, fn_png1, log1p_xy = FALSE, bins = bins)
    if (isTRUE(make_pdf) && !identical(g1, FALSE)) print(g1)
    
    # log1p スケール
    fn_png2 <- file.path(out_dir, sprintf("%s_vs_%s_log.png", y, x))
    g2 <- .save_scatter(DT, x, y, fn_png2, log1p_xy = TRUE, bins = bins)
    if (isTRUE(make_pdf) && !identical(g2, FALSE)) print(g2)
    
    message(sprintf("[OK] %s ~ %s -> %s / %s", y, x, fn_png1, fn_png2))
  }
  
  # 参考: IV 第一段階（alerts ~ cloud_share）
  if ("alerts" %in% names(DT)) {
    iv_x <- intersect(c("cloud_share","cloud_prob","cloud"), names(DT))
    if (length(iv_x)) {
      x <- iv_x[[1]]
      fn <- file.path(out_dir, sprintf("alerts_vs_%s_log.png", x))
      g_iv <- .save_scatter(DT, x, "alerts", fn, log1p_xy = TRUE, bins = bins)
      if (isTRUE(make_pdf) && !identical(g_iv, FALSE)) print(g_iv)
      message(sprintf("[OK] alerts ~ %s (第一段階確認) -> %s", x, fn))
    }
  }
  
  invisible(list(y_used = y, x_used = xs,
                 out_dir = out_dir,
                 pdf_path = if (isTRUE(make_pdf)) pdf_path else NA_character_))
}

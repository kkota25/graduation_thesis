# scripts/06_histograms.R
# ------------------------------------------------------------
# 目的: 「アラート以外の数値列」ごとにヒストグラムを作成し保存
# 入力: data/processed/adm2_panel.parquet
# 出力: 
#   - outputs/figures/hist/<variable>_hist.png
#   - reports/outputs/figures/histograms_all.pdf（全変数まとめ）
# 依存: data.table, arrow, here, ggplot2, fs
# ------------------------------------------------------------

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
  library(here)
  library(ggplot2)
  library(fs)
})

# ---- 対象列の選別（05_descriptives と同じ規則） ----
.select_target_vars <- function(DT){
  id_like    <- grepl("^adm\\d|_code$|_name$|^year$|^geometry$", names(DT), ignore.case = TRUE)
  alert_like <- grepl("alert", names(DT), ignore.case = TRUE)
  is_num     <- vapply(DT, function(x) is.numeric(x) || is.integer(x), logical(1))
  names(DT)[is_num & !id_like & !alert_like]
}

# ---- ファイル名を安全化（プレースホルダ不使用） ----
.safe_fname <- function(x){
  x <- gsub("[^[:alnum:]_]+","_", x)
  x <- gsub("_+","_", x)
  x <- sub("^_","", x)
  x <- sub("_$","", x)
  x
}

# ---- メイン関数 ----
run_histograms <- function(panel_path,
                           out_dir_png = here("outputs","figures","hist"),
                           make_pdf    = TRUE,
                           pdf_path    = here("reports","outputs","figures","histograms_all.pdf"),
                           bins        = 30,
                           facet_year  = FALSE){
  if (!file.exists(panel_path)) stop("パネルファイルが見つかりません: ", panel_path)
  
  fs::dir_create(out_dir_png, recurse = TRUE)
  fs::dir_create(fs::path_dir(pdf_path), recurse = TRUE)
  
  DT <- as.data.table(arrow::read_parquet(panel_path))
  vars <- .select_target_vars(DT)
  if (length(vars) == 0L) stop("対象となる数値列（アラート以外）が見つかりません。")
  
  if (isTRUE(make_pdf)) {
    grDevices::pdf(pdf_path, width = 8.5, height = 6.0, onefile = TRUE, paper = "special")
    on.exit(try(grDevices::dev.off(), silent = TRUE), add = TRUE)
  }
  
  for (v in vars) {
    x <- DT[[v]]
    ok <- is.finite(x)
    n_ok <- sum(ok, na.rm = TRUE)
    if (n_ok == 0L) { message(sprintf("[SKIP] %s: 有効値なし", v)); next }
    
    df <- data.frame(val = x[ok])
    g <- ggplot(df, aes(val)) +
      geom_histogram(bins = bins, linewidth = 0.2) +
      labs(title = sprintf("%s (n=%d)", v, n_ok), x = v, y = "count") +
      theme_minimal(base_size = 12) +
      theme(panel.grid.minor = element_blank())
    
    if (facet_year) {
      if ("year" %in% names(DT)) {
        df$year <- DT$year[ok]
        g <- ggplot(df, aes(val)) +
          geom_histogram(bins = bins, linewidth = 0.2) +
          facet_wrap(~ year, scales = "free_y") +
          labs(title = sprintf("%s by year (n=%d)", v, n_ok), x = v, y = "count") +
          theme_minimal(base_size = 11) +
          theme(panel.grid.minor = element_blank())
      } else {
        warning("year 列がないため facet_year は無視します。")
      }
    }
    
    fn_png <- file.path(out_dir_png, sprintf("%s_hist%s.png",
                                             .safe_fname(v),
                                             if (facet_year) "_byyear" else ""))
    ggsave(filename = fn_png, plot = g, width = 8.5, height = 6.0, dpi = 150)
    message(sprintf("[OK] %s -> %s", v, fn_png))
    
    if (isTRUE(make_pdf)) print(g)
  }
  
  invisible(list(vars_used = vars,
                 png_dir   = out_dir_png,
                 pdf_path  = if (isTRUE(make_pdf)) pdf_path else NA_character_))
}


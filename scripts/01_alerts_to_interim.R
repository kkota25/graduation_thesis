# scripts/01_alerts_to_interim.R
# ------------------------------------------------------------
# 目的:
#   すでに GEE からエクスポート済みの Integrated Deforestation Alerts CSV
#   （data/raw/alerts 以下）を読み込み、
#   ADM2_CODE × year のロング形式に整理して
#   data/interim/alerts_year.parquet を作成する。
#
# 前提:
#   - alerts の CSV は data/raw/alerts に格納。
#   - 列構成は共通で以下:
#       system:index, adm1_name, adm2_code, adm2_name, ha_alerts, year, .geo
#   - 2019〜2025 年のファイル:
#       idn_integrated_alerts_adm2_yearly_2019.csv
#       ...
#       idn_integrated_alerts_adm2_yearly_2025.csv
#
# 実行:
#   RStudio Console から:
#     source("scripts/01_alerts_to_interim.R")
#     alerts_year <- run_build_alerts_from_raw()
# ------------------------------------------------------------

suppressPackageStartupMessages({
  library(data.table)
  library(dplyr)
  library(fs)
  library(here)
  library(arrow)
})

# 共通ユーティリティ（fast_read_csv(), write_parquet_safely() など）
source("R/utils_io.R")

# ===== ヘルパー関数 ==========================================

.read_one_alerts_csv <- function(path) {
  # 読み込み
  dt <- fast_read_csv(path)
  dt <- as.data.table(dt)
  
  # 必須列の確認（今回のスキーマに完全依存）
  required_cols <- c("adm2_code", "year", "ha_alerts",
                     "adm1_name", "adm2_name")
  if (!all(required_cols %in% names(dt))) {
    stop(
      "必須列が見つかりません: ",
      path, "\n必要: ", paste(required_cols, collapse = ", "),
      "\n実際: ", paste(names(dt), collapse = ", ")
    )
  }
  
  # 型をそろえる
  dt[, adm2_code := as.integer(adm2_code)]
  dt[, year      := as.integer(year)]
  dt[, ha_alerts := suppressWarnings(as.numeric(ha_alerts))]
  dt[, adm1_name := as.character(adm1_name)]
  dt[, adm2_name := as.character(adm2_name)]
  
  # 必要な列だけ残す（system:index, .geo は捨てる）
  dt <- dt[, .(adm2_code, year, adm1_name, adm2_name, ha_alerts)]
  
  dt[]
}

# ===== メイン関数 ============================================

run_build_alerts_from_raw <- function(
    raw_dir      = here("data", "raw", "alerts"),
    out_parquet  = here("data", "interim", "alerts_year.parquet")
) {
  # ディレクトリ存在確認
  if (!dir_exists(raw_dir)) {
    stop("raw alerts ディレクトリが存在しません: ", raw_dir)
  }
  
  # CSV ファイル一覧（サブディレクトリは見ない）
  files <- dir_ls(raw_dir, glob = "*.csv", type = "file", recurse = FALSE)
  
  if (length(files) == 0L) {
    stop("data/raw/alerts に CSV ファイルが見つかりません。")
  }
  
  message("読み込むファイル数: ", length(files))
  print(basename(files))
  
  # 各 CSV を読み込み
  out_list <- vector("list", length(files))
  for (i in seq_along(files)) {
    message("  [", i, "/", length(files), "] ", basename(files[i]))
    out_list[[i]] <- .read_one_alerts_csv(files[i])
  }
  
  # 全結合
  dt <- data.table::rbindlist(out_list, use.names = TRUE, fill = TRUE)
  
  # 念のため、年の範囲を 2019〜2025 に限定（他の年が紛れ込んでいても無視）
  dt <- dt[year >= 2019 & year <= 2025]
  
  # ADM2×year で合計（基本的には各ファイルで 1 行/ADM2/year なので、
  # ここは行がそのまま維持されるはずだが、保険として sum を取る）
  agg <- dt[, .(
    adm1_name = adm1_name[1L],
    adm2_name = adm2_name[1L],
    ha_alerts = sum(ha_alerts, na.rm = TRUE)
  ), by = .(adm2_code, year)]
  
  # 列順と並び替え
  data.table::setcolorder(agg, c("adm2_code", "year", "adm1_name", "adm2_name", "ha_alerts"))
  data.table::setorder(agg, adm2_code, year)
  
  # Parquet に保存
  write_parquet_safely(agg, out_parquet)
  message("書き出し完了: ", out_parquet)
  message(" 行数: ", nrow(agg))
  
  invisible(agg[])
}

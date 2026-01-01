# scripts/13_clean_chirps.R
# CHIRPS の raw CSV を 1 本の parquet (chirps_year.parquet) に整形

rm(list = ls())

library(here)
library(data.table)
library(arrow)

# fast_read_csv(), write_parquet_safely() を使う
source(here("R", "utils_io.R"))

# ------------------------------------------------------------
# 入力ファイル
#  - 元データ: idn_chirps_adm2_2019_2025.csv
# ------------------------------------------------------------
# プロジェクトルートからの相対パス
chirps_csv <- here("data", "raw", "idn_chirps_adm2_2019_2025.csv")


if (!file.exists(chirps_csv)) {
  stop("CHIRPS の CSV が見つかりません: ", chirps_csv)
}

# ------------------------------------------------------------
# 読み込み・整形
# ------------------------------------------------------------
dt <- fast_read_csv(chirps_csv)
setDT(dt)

# 列名を統一
#  ADM1_NAME / ADM2_CODE / ADM2_NAME → adm1_name / adm2_code / adm2_name
setnames(
  dt,
  old = c("ADM1_NAME", "ADM2_CODE", "ADM2_NAME"),
  new = c("adm1_name", "adm2_code", "adm2_name"),
  skip_absent = TRUE
)

# 降水量の列名をわかりやすく変更
#  calendar_year_mm  → chirps_mm
#  rainy_season_mm   → chirps_rainy_mm
setnames(
  dt,
  old = c("calendar_year_mm", "rainy_season_mm"),
  new = c("chirps_mm",       "chirps_rainy_mm"),
  skip_absent = TRUE
)

# 型をそろえる
if ("year" %in% names(dt)) {
  dt[, year := as.integer(year)]
}
for (nm in c("chirps_mm", "chirps_rainy_mm")) {
  if (nm %in% names(dt)) {
    dt[[nm]] <- as.numeric(dt[[nm]])
  }
}

# 不要な列を削除（system:index, .geo など）
drop_cols <- intersect(c("system:index", ".geo"), names(dt))
if (length(drop_cols) > 0L) {
  dt[, (drop_cols) := NULL]
}

# 必要な列だけにそろえ、順番も整理
keep <- c("adm1_name", "adm2_code", "adm2_name",
          "year", "chirps_mm", "chirps_rainy_mm")
keep <- intersect(keep, names(dt))
dt <- dt[, ..keep]

# ------------------------------------------------------------
# parquet に書き出し
# ------------------------------------------------------------
out_path <- here("data", "interim", "chirps_year.parquet")

write_parquet_safely(
  dt,
  out_path
)

cat("CHIRPS →", out_path, "を作成しました\n")

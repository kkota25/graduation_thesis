# scripts/06_clean_hotspot.R
# data/raw/hotspot の CSV を 1 本の parquet に整形して保存
# 出力: data/interim/hotspot_year.parquet
# 列: adm1_name, adm2_code, adm2_name, year, fire_cnt, frp_sum, hotspot_n

rm(list = ls())

library(here)
library(data.table)
library(arrow)

# fast_read_csv(), write_parquet_safely() を使う
source(here("R", "utils_io.R"))

# ------------------------------------------------------------
# hotspot CSV をすべて読み込み & 結合
#   例: idn_viirs_fire_adm2_Java_Bali_2019_2021_2019_2021_conf1.csv
#   ファイル名に含まれる地域名・期間は使わない（無視）
# ------------------------------------------------------------
hotspot_dir <- here("data", "raw", "hotspot")
files <- list.files(hotspot_dir, pattern = "\\.csv$", full.names = TRUE)

if (length(files) == 0L) {
  stop("hotspot の CSV ファイルが見つかりません: ", hotspot_dir)
}

hotspot_list <- lapply(files, function(path) {
  dt <- fast_read_csv(path)
  setDT(dt)
  
  # 列名を統一
  setnames(
    dt,
    old = c("ADM1_NAME", "ADM2_CODE", "ADM2_NAME"),
    new = c("adm1_name", "adm2_code", "adm2_name"),
    skip_absent = TRUE
  )
  
  # ---- year の整形（元データの year を必ず残す）----
  if (!"year" %in% names(dt)) {
    stop("year 列が見つかりません: ", basename(path))
  }
  # 文字列の可能性もあるので、一度 character にしてから整数化
  dt[, year := suppressWarnings(as.integer(as.character(year)))]
  
  # ---- fire_cnt / frp_sum の整形 ----
  if (!"fire_cnt" %in% names(dt)) {
    stop("fire_cnt 列が見つかりません: ", basename(path))
  }
  # 既に numeric であっても問題ない（as.numeric はそのまま返す）
  dt[, fire_cnt := suppressWarnings(as.numeric(fire_cnt))]
  # alerts との整合をとるための別名
  dt[, hotspot_n := fire_cnt]
  
  if ("frp_sum" %in% names(dt)) {
    dt[, frp_sum := suppressWarnings(as.numeric(frp_sum))]
  }
  
  # ---- 不要な列を削除（system:index, .geo など。ただし year は消さない）----
  drop_cols <- intersect(c("system:index", ".geo"), names(dt))
  if (length(drop_cols) > 0L) {
    dt[, (drop_cols) := NULL]
  }
  
  # ---- 必要な列だけに揃える（year を必ず含める）----
  keep <- c(
    "adm1_name", "adm2_code", "adm2_name",
    "year", "fire_cnt", "frp_sum", "hotspot_n"
  )
  keep <- intersect(keep, names(dt))
  dt <- dt[, ..keep]
  
  dt[]
})

hotspot_all <- rbindlist(hotspot_list, use.names = TRUE, fill = TRUE)

# ------------------------------------------------------------
# parquet に書き出し
# ------------------------------------------------------------
out_path <- here("data", "interim", "hotspot_year.parquet")

write_parquet_safely(
  hotspot_all,
  out_path
)

cat("hotspot →", out_path, "を作成しました\n")

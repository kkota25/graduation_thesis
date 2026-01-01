# scripts/14_clean_clouds.R
# clouds の raw CSV（data/raw/clouds）を 1 本の parquet に整形して保存
# → 出力: data/interim/clouds_period.parquet
#   adm1_name, adm2_code, adm2_name, region, period, year, cloud_share, clear_share

rm(list = ls())

library(here)
library(data.table)
library(arrow)

# fast_read_csv(), write_parquet_safely() を使う
source(here("R", "utils_io.R"))

# ------------------------------------------------------------
# ファイル名から region と period を取り出すヘルパ
# 例:
#   IDN_cloud_ADM2_Java_Bali_2019_2021.csv
#   IDN_cloud_ADM2_Kalimantan_2022_2025.csv
#
# → region = "Java Bali" / "Kalimantan"
#   period = "2019_2021" / "2022_2025"
# ------------------------------------------------------------
extract_region_period_cloud <- function(path) {
  fname <- basename(path)
  fname <- sub("\\.csv$", "", fname)
  fname <- sub("^IDN_cloud_ADM2_", "", fname)
  # ここまでで:
  #   Java_Bali_2019_2021
  #   Kalimantan_2022_2025
  #   Maluku_NT_2019_2021
  # など
  
  parts <- strsplit(fname, "_", fixed = TRUE)[[1]]
  n <- length(parts)
  
  # region (>=1 語) + start_year + end_year → 最低 3 要素必要
  if (n < 3L) {
    stop("想定外のファイル名です: ", fname)
  }
  
  # 末尾 2 要素が年情報 (2019, 2021) の想定
  region_tokens <- parts[1:(n - 2L)]
  period_tokens <- parts[(n - 1L):n]
  
  region <- paste(region_tokens, collapse = "_")
  region <- gsub("_", " ", region)                # Java Bali, Maluku NT など
  period <- paste(period_tokens, collapse = "_")  # "2019_2021"
  
  list(region = region, period = period)
}

# ------------------------------------------------------------
# cloud CSV をすべて読み込み & 結合
# ------------------------------------------------------------
cloud_dir <- here("data", "raw", "clouds")
files <- list.files(cloud_dir, pattern = "\\.csv$", full.names = TRUE)

if (length(files) == 0L) {
  stop("cloud の CSV ファイルが見つかりません: ", cloud_dir)
}

cloud_list <- lapply(files, function(path) {
  meta <- extract_region_period_cloud(path)
  
  dt <- fast_read_csv(path)
  setDT(dt)
  
  # 列名を統一
  setnames(
    dt,
    old = c("ADM1_NAME", "ADM2_CODE", "ADM2_NAME"),
    new = c("adm1_name", "adm2_code", "adm2_name"),
    skip_absent = TRUE
  )
  
  # year 列（元データ）をそのまま使う
  if (!"year" %in% names(dt)) {
    stop("year 列が見つかりません: ", basename(path))
  }
  dt[, year := as.integer(year)]
  
  # cloud / clear 割合（列名は元データに合わせて調整）
  # ここでは cloud_share / clear_share という列名を仮定
  if ("cloud_share" %in% names(dt)) {
    dt[, cloud_share := as.numeric(cloud_share)]
  }
  if ("clear_share" %in% names(dt)) {
    dt[, clear_share := as.numeric(clear_share)]
  }
  
  # 不要な列を削除（system:index, .geo など）
  drop_cols <- intersect(c("system:index", ".geo"), names(dt))
  if (length(drop_cols) > 0L) {
    dt[, (drop_cols) := NULL]
  }
  
  # region / period を付与
  dt[, region := meta$region]
  dt[, period := meta$period]
  
  # 必要な列だけに揃える（year を必ず含める）
  keep <- c("adm1_name", "adm2_code", "adm2_name",
            "region", "period", "year",
            "cloud_share", "clear_share")
  keep <- intersect(keep, names(dt))
  dt <- dt[, ..keep]
  
  dt[]
})

cloud_all <- rbindlist(cloud_list, use.names = TRUE, fill = TRUE)

# ------------------------------------------------------------
# parquet に書き出し
# ------------------------------------------------------------
out_path <- here("data", "interim", "clouds_year.parquet")

write_parquet_safely(
  cloud_all,
  out_path
)

cat("clouds →", out_path, "を作成しました\n")


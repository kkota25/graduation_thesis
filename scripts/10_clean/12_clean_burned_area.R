# scripts/12_clean_burned_area.R
# burned area の raw CSV（data/raw/burned_area）を 1 本の parquet にまとめる

rm(list = ls())

library(here)
library(data.table)
library(arrow)

# fast_read_csv(), write_parquet_safely() を使う
source(here("R", "utils_io.R"))

# ------------------------------------------------------------
# ファイル名から region と period を取り出すヘルパ
# 例: idn_burned_ha_adm2_Java_Bali_2019_2021_2019_2021.csv
#   → region = "Java Bali", period = "2019_2021"
# ------------------------------------------------------------
extract_region_period <- function(path) {
  fname <- basename(path)
  fname <- sub("\\.csv$", "", fname)
  fname <- sub("^idn_burned_ha_adm2_", "", fname)
  # ここまでで:
  # Java_Bali_2019_2021_2019_2021
  # Kalimantan_2019_2021_2019_2021
  # などになる
  
  parts <- strsplit(fname, "_", fixed = TRUE)[[1]]
  n <- length(parts)
  # region が 1 語（Kalimantan など）の場合でも n = 5 なので、5 未満だけエラーにする
  if (n < 5L) {
    stop("想定外のファイル名です: ", fname)
  }
  
  # 末尾 4 要素が年情報 (2019, 2021, 2019, 2021) になっている想定
  region_tokens <- parts[1:(n - 4L)]              # Java_Bali / Kalimantan / Maluku_NT ...
  period_tokens <- parts[(n - 3L):(n - 2L)]       # 2019, 2021 など
  
  region <- paste(region_tokens, collapse = "_")
  region <- gsub("_", " ", region)                # Java Bali, Maluku NT など
  period <- paste(period_tokens, collapse = "_")  # "2019_2021"
  
  list(region = region, period = period)
}

# ------------------------------------------------------------
# burned area CSV をすべて読み込み & 結合
# ------------------------------------------------------------
burned_dir <- here("data", "raw", "burned_area")
files <- list.files(burned_dir, pattern = "\\.csv$", full.names = TRUE)

if (length(files) == 0L) {
  stop("burned area の CSV ファイルが見つかりません: ", burned_dir)
}

burned_list <- lapply(files, function(path) {
  meta <- extract_region_period(path)
  
  dt <- fast_read_csv(path)
  setDT(dt)
  
  # 列名を統一
  setnames(
    dt,
    old = c("ADM1_NAME", "ADM2_CODE", "ADM2_NAME"),
    new = c("adm1_name", "adm2_code", "adm2_name"),
    skip_absent = TRUE
  )
  
  # 型をそろえる
  if ("burned_ha" %in% names(dt)) {
    dt[, burned_ha := as.numeric(burned_ha)]
  }
  if ("year" %in% names(dt)) {
    dt[, year := as.integer(year)]
  }
  
  # 不要な列を削除（必要なら残してもよい）
  drop_cols <- intersect(c("system:index", ".geo"), names(dt))
  if (length(drop_cols) > 0L) {
    dt[, (drop_cols) := NULL]
  }
  
  # region / period を付与（year はあくまで CSV 内の列を使う）
  dt[, region := meta$region]
  dt[, period := meta$period]
  
  # 必要な列だけにそろえる
  keep <- c("adm1_name", "adm2_code", "adm2_name",
            "region", "period", "year", "burned_ha")
  keep <- intersect(keep, names(dt))
  dt[, ..keep][]
})

burned_all <- rbindlist(burned_list, use.names = TRUE, fill = TRUE)

# ------------------------------------------------------------
# parquet に書き出し
# ------------------------------------------------------------
write_parquet_safely(
  burned_all,
  here("data", "interim", "burned_period.parquet")
)

cat("burned area → data/interim/burned_period.parquet を作成しました\n")


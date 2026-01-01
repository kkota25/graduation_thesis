# scripts/15_clean_forestloss.R
# data/raw/forestloss の CSV を 1 本の parquet に整形して保存
# 出力: data/interim/forestloss_year.parquet
# 列: adm1_name, adm2_code, adm2_name, year, defor_rate, forest2000_ha, loss_ha

rm(list = ls())

library(here)
library(data.table)
library(arrow)

# fast_read_csv(), write_parquet_safely() を使う
source(here("R", "utils_io.R"))

# ------------------------------------------------------------
# forestloss CSV をすべて読み込み & 結合
#   ファイル名に含まれる地域名や期間は使わない（無視）
# ------------------------------------------------------------
forest_dir <- here("data", "raw", "forestloss")
files <- list.files(forest_dir, pattern = "\\.csv$", full.names = TRUE)

if (length(files) == 0L) {
  stop("forestloss の CSV ファイルが見つかりません: ", forest_dir)
}

forest_list <- lapply(files, function(path) {
  dt <- fast_read_csv(path)
  setDT(dt)
  
  # 列名を統一
  setnames(
    dt,
    old = c("ADM1_NAME", "ADM2_CODE", "ADM2_NAME"),
    new = c("adm1_name", "adm2_code", "adm2_name"),
    skip_absent = TRUE
  )
  
  # year / defor_rate / forest2000_ha / loss_ha の型をそろえる
  if (!"year" %in% names(dt)) {
    stop("year 列が見つかりません: ", basename(path))
  }
  dt[, year := as.integer(year)]
  
  for (nm in c("defor_rate", "forest2000_ha", "loss_ha")) {
    if (nm %in% names(dt)) {
      dt[[nm]] <- as.numeric(dt[[nm]])
    }
  }
  
  # 不要な列を削除（system:index, .geo など）
  drop_cols <- intersect(c("system:index", ".geo"), names(dt))
  if (length(drop_cols) > 0L) {
    dt[, (drop_cols) := NULL]
  }
  
  # 必要な列だけに揃える
  keep <- c(
    "adm1_name", "adm2_code", "adm2_name",
    "year", "defor_rate", "forest2000_ha", "loss_ha"
  )
  keep <- intersect(keep, names(dt))
  dt <- dt[, ..keep]
  
  dt[]
})

forest_all <- rbindlist(forest_list, use.names = TRUE, fill = TRUE)

# ------------------------------------------------------------
# parquet に書き出し
# ------------------------------------------------------------
out_path <- here("data", "interim", "forestloss_year.parquet")

write_parquet_safely(
  forest_all,
  out_path
)

cat("forestloss →", out_path, "を作成しました\n")


# scripts/03_build_processed.R
# 期間→年へ展開→結合→派生列→保存

req <- c("data.table","arrow","fs","fixest")
ins <- req[!sapply(req, requireNamespace, quietly = TRUE)]
if (length(ins)) install.packages(ins)
library(data.table); library(arrow); library(fs); library(fixest)

source("R/utils_io.R")  # years_in_period(), write_parquet_safely()

interim <- "data/interim"
outdir  <- "data/processed"
dir_create(outdir)

# ---------- 読み込み ----------
clouds   <- as.data.table(read_parquet(file.path(interim,"clouds_period.parquet")))
burned   <- as.data.table(read_parquet(file.path(interim,"burned_period.parquet")))
forest   <- as.data.table(read_parquet(file.path(interim,"forestloss_period.parquet")))
hotspot  <- as.data.table(read_parquet(file.path(interim,"hotspot_period.parquet")))
alerts   <- as.data.table(read_parquet(file.path(interim,"alerts_year.parquet")))
chirps   <- as.data.table(read_parquet(file.path(interim,"chirps_year.parquet")))
viirs    <- as.data.table(read_parquet(file.path(interim,"viirs_year.parquet")))


# 3) 期間→年 展開（utils_io.R の expand_period を使用）
clouds_exp  <- expand_period(clouds, c("cloud_share","clear_share"), how="copy")
burned_exp  <- expand_period(burned, "burned_ha",  how="split")
forest_exp  <- expand_period(forest, "loss_ha",    how="split")
hotspot_exp <- expand_period(hotspot,"hotspot_n",  how="split")


# 4) ADM2×yearで重複を潰してからマージ（cartesian回避）

library(data.table)

# (a) 名前テーブル（後でadm2_codeだけで付与）
name_tbl <- unique(clouds_exp[, .(adm2_code, adm1_name, adm2_name, region)])

# (b) 比率・平均系は平均で集約
clouds_u <- clouds_exp[, .(
  cloud_share = mean(cloud_share, na.rm = TRUE),
  clear_share = mean(clear_share, na.rm = TRUE)
), by = .(adm2_code, year)]

chirps_u <- if ("chirps_rainy_mm" %in% names(chirps)) {
  chirps[, .(
    chirps_mm       = mean(chirps_mm, na.rm = TRUE),
    chirps_rainy_mm = mean(chirps_rainy_mm, na.rm = TRUE)
  ), by = .(adm2_code, year)]
} else {
  chirps[, .(
    chirps_mm = mean(chirps_mm, na.rm = TRUE)
  ), by = .(adm2_code, year)]
}

viirs_u  <- viirs[, .(viirs_ntl = mean(viirs_ntl, na.rm = TRUE)),
                  by = .(adm2_code, year)]

# (c) 件数・面積系は合計で集約
burned_u  <- burned_exp[,  .(burned_ha = sum(burned_ha, na.rm = TRUE)),
                        by = .(adm2_code, year)]
forest_u  <- forest_exp[,  .(loss_ha   = sum(loss_ha,   na.rm = TRUE)),
                        by = .(adm2_code, year)]
hotspot_u <- hotspot_exp[, .(hotspot_n = sum(hotspot_n, na.rm = TRUE)),
                         by = .(adm2_code, year)]
alerts_u <- alerts[, .(alerts = sum(alerts, na.rm=TRUE)), by = .(adm2_code, year)]

###

# --- マージ前にキー型を統一 ---
fix_key_types <- function(dt){
  dt[, adm2_code := as.character(adm2_code)]
  dt[, year      := as.integer(year)]
  dt
}
clouds_u  <- fix_key_types(clouds_u)
burned_u  <- fix_key_types(burned_u)
forest_u  <- fix_key_types(forest_u)
hotspot_u <- fix_key_types(hotspot_u)
alerts_u  <- fix_key_types(alerts_u)
chirps_u  <- fix_key_types(chirps_u)
viirs_u   <- fix_key_types(viirs_u)

# 念のため NA キーが無いか簡易チェック（任意）
for (nm in c("clouds_u","burned_u","forest_u","hotspot_u","alerts_u","chirps_u","viirs_u")){
  dt <- get(nm)
  if (any(is.na(dt$adm2_code) | is.na(dt$year))) {
    warning(nm, ": NA keys detected → その行はマージで落ちます")
  }
}


# (d) マージ（すべてuniqueになったので直積は発生しない）
setkey(clouds_u, adm2_code, year)
panel <- Reduce(function(x,y) merge(x, y, by=c("adm2_code","year"), all=TRUE), list(
  clouds_u, burned_u, forest_u, hotspot_u, alerts_u, chirps_u, viirs_u
))

# (e) 名前情報を付与（adm2_codeで）
# 1) clouds_exp から adm2_codeごとに1行だけの name_tbl を作る
name_tbl <- clouds_exp[
  , .(
    adm1_name = na.omit(adm1_name)[1],
    adm2_name = na.omit(adm2_name)[1],
    region    = na.omit(region)[1]
  ),
  by = .(adm2_code)
]

# 2) 型を統一（両方とも character に）
panel[,    adm2_code := as.character(adm2_code)]
name_tbl[, adm2_code := as.character(adm2_code)]

# 3) マージ
panel <- merge(panel, name_tbl, by = "adm2_code", all.x = TRUE)


# (f) 派生列・QC・保存
# 先に alerts_adj を作成
panel[, alerts_adj := fifelse(!is.na(clear_share) & clear_share > 0,
                              alerts/clear_share, NA_real_)]

# その後でログ列を作成
panel[, `:=`(
  ln_alerts_adj = log1p(alerts_adj),
  ln_loss       = log1p(loss_ha),
  ln_burn       = log1p(burned_ha),
  ln_hotspot    = log1p(hotspot_n)
)]

dup <- panel[, .N, by = .(adm2_code, year)][N > 1]
stopifnot(nrow(dup) == 0)

write_parquet_safely(panel, "data/processed/adm2_panel.parquet")
message("✅ saved: data/processed/adm2_panel.parquet  (rows=", nrow(panel), ")")

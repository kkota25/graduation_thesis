# scripts/02_clean_to_interim.R
# -----------------------------------------------
# 目的: data/raw/ 配下のCSVを正規化して data/interim/*.parquet を作成
# 前提: R/utils_io.R を用意済み（fast_read_csv, write_parquet_safely など）
# -----------------------------------------------

# ---- packages ----
req <- c("data.table","arrow","fs","stringr")
ins <- req[!sapply(req, requireNamespace, quietly = TRUE)]
if (length(ins)) install.packages(ins)
library(data.table); library(arrow); library(fs); library(stringr)

# ---- utils ----
source("R/utils_io.R")  # fast_read_csv(), write_parquet_safely(), parse_region_period(), years_in_period()

# 安全リネーム（utils_io.R に無ければここで使う）
safe_rename <- function(dt, candidates, new) {
  nm <- names(dt); hit <- candidates[candidates %in% nm]
  if (length(hit) == 0) return(invisible(FALSE))
  data.table::setnames(dt, hit[1], new); TRUE
}

# ---- paths ----
root_raw <- "data/raw"
out_dir  <- "data/interim"
dir_create(out_dir)

# ========= clouds（期間平均: cloud_share / clear_share） =========
cloud_files <- dir_ls(file.path(root_raw, "clouds"), glob = "*.csv")
message("clouds files: ", length(cloud_files))
cloud_list <- lapply(seq_along(cloud_files), function(i){
  f <- cloud_files[i]; meta <- parse_region_period(basename(f))
  message(sprintf("[clouds %d/%d] %s", i, length(cloud_files), basename(f)))
  dt <- fast_read_csv(f); setnames(dt, tolower(names(dt)))
  
  safe_rename(dt, c("adm1_name","adm1"), "adm1_name")
  safe_rename(dt, c("adm2_code","adm2id","adm2"), "adm2_code")
  safe_rename(dt, c("adm2_name","adm2"), "adm2_name")
  safe_rename(dt, c("cloud_share","cloud"), "cloud_share")
  safe_rename(dt, c("clear_share","clear"), "clear_share")
  
  dt[, `:=`(region = meta$region, period = meta$period)]
  dt[, .(adm1_name, adm2_code, adm2_name, region, period, cloud_share, clear_share)]
})
clouds <- rbindlist(cloud_list, use.names = TRUE, fill = TRUE)
write_parquet_safely(clouds, file.path(out_dir, "clouds_period.parquet"))
message("✅ clouds_period.parquet: ", nrow(clouds), " rows")

# ========= burned_area（期間合計 ha） =========
burn_files <- dir_ls(file.path(root_raw, "burned_area"), glob = "*.csv")
message("burned_area files: ", length(burn_files))
burn_list <- lapply(seq_along(burn_files), function(i){
  f <- burn_files[i]; meta <- parse_region_period(basename(f))
  message(sprintf("[burned %d/%d] %s", i, length(burn_files), basename(f)))
  dt <- fast_read_csv(f); setnames(dt, tolower(names(dt)))
  
  safe_rename(dt, c("adm1_name","adm1"), "adm1_name")
  safe_rename(dt, c("adm2_code","adm2id","adm2"), "adm2_code")
  safe_rename(dt, c("adm2_name","adm2"), "adm2_name")
  safe_rename(dt, c("burned_ha","burned_area","area_burned_ha"), "burned_ha")
  
  dt[, `:=`(region = meta$region, period = meta$period)]
  dt[, .(adm1_name, adm2_code, adm2_name, region, period, burned_ha)]
})
burned <- rbindlist(burn_list, use.names = TRUE, fill = TRUE)
write_parquet_safely(burned, file.path(out_dir, "burned_period.parquet"))
message("✅ burned_period.parquet: ", nrow(burned), " rows")

# ========= forestloss（期間合計 ha） =========
floss_files <- dir_ls(file.path(root_raw, "forestloss"), glob = "*.csv")
message("forestloss files: ", length(floss_files))
floss_list <- lapply(seq_along(floss_files), function(i){
  f <- floss_files[i]; meta <- parse_region_period(basename(f))
  message(sprintf("[forestloss %d/%d] %s", i, length(floss_files), basename(f)))
  dt <- fast_read_csv(f); setnames(dt, tolower(names(dt)))
  
  safe_rename(dt, c("adm1_name","adm1"), "adm1_name")
  safe_rename(dt, c("adm2_code","adm2id","adm2"), "adm2_code")
  safe_rename(dt, c("adm2_name","adm2"), "adm2_name")
  safe_rename(dt, c("loss_ha","gfc_loss_ha","forestloss_ha"), "loss_ha")
  
  dt[, `:=`(region = meta$region, period = meta$period)]
  dt[, .(adm1_name, adm2_code, adm2_name, region, period, loss_ha)]
})
forestloss <- rbindlist(floss_list, use.names = TRUE, fill = TRUE)
write_parquet_safely(forestloss, file.path(out_dir, "forestloss_period.parquet"))
message("✅ forestloss_period.parquet: ", nrow(forestloss), " rows")

# ========= hotspot（期間合計 件数） =========
hot_files <- dir_ls(file.path(root_raw, "hotspot"), glob = "*.csv")
message("hotspot files: ", length(hot_files))
hot_list <- lapply(seq_along(hot_files), function(i){
  f <- hot_files[i]; meta <- parse_region_period(basename(f))
  message(sprintf("[hotspot %d/%d] %s", i, length(hot_files), basename(f)))
  dt <- fast_read_csv(f); setnames(dt, tolower(names(dt)))
  
  safe_rename(dt, c("adm1_name","adm1"), "adm1_name")
  safe_rename(dt, c("adm2_code","adm2id","adm2"), "adm2_code")
  safe_rename(dt, c("adm2_name","adm2"), "adm2_name")
  
  found <- safe_rename(dt, c("hotspot_n","hotspots","hotspot","count","n_hotspot",
                             "n","total","fire_count","viirs_hotspot_count"), "hotspot_n")
  if (!found) { warning("hotspot列が見つからず NA で作成: ", basename(f)); dt[, hotspot_n := NA_real_] }
  
  dt[, `:=`(region = meta$region, period = meta$period)]
  dt[, .(adm1_name, adm2_code, adm2_name, region, period, hotspot_n)]
})
hotspot <- rbindlist(hot_list, use.names = TRUE, fill = TRUE)
write_parquet_safely(hotspot, file.path(out_dir, "hotspot_period.parquet"))
message("✅ hotspot_period.parquet: ", nrow(hotspot), " rows")

# ========= alerts（年次, 170MB想定） =========
# scripts/02_ingest_alerts.R
# 役割: raw配下のアラート関連ファイルを読み込み → 変数名を正規化 → 
#       ADM2×year で集計 → parquet保存（processed配下）
suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
  library(here)
})
source(here("R/utils_io.R"))  # safe_rename を使用

norm_alerts <- function(dt){
  # 別名を安全に正規化（存在する列だけをrename）
  safe_rename(dt, c("ADM2_CODE","adm2_code","adm2"), "adm2_code")
  safe_rename(dt, c("year","calendar_year","YYYY"), "year")
  safe_rename(dt, c("ha_alerts","integrated_alerts","alerts_ha","alerts"), "alerts")
  
  # 型を明示
  dt[, alerts   := as.numeric(alerts)]
  dt[, year     := as.integer(year)]
  # adm2_code が文字列のケースもあるので明示的に数値化（失敗時はNA）
  dt[, adm2_code := as.character(adm2_code)]
  
  # 必要列だけに絞り、完全NAを除去
  dt <- dt[, .(adm2_code, year, alerts)]
  dt[!is.na(adm2_code) & !is.na(year) & !is.na(alerts)]
}

# ---- メイン：関数化 ----------------------------------------------
run_ingest_alerts <- function(
    in_dir = here("data/raw"),
    out_par = here("data/interim/alerts_year.parquet")
){
  files <- list.files(
    in_dir,
    pattern = "idn_integrated_alerts_adm2_yearly_.*\\.csv$",
    recursive = TRUE, full.names = TRUE
  )
  if (length(files) == 0L) {
    stop(sprintf("No files matching 'alert' under: %s", in_dir))
  }
  
  read_one <- function(f){
    ext <- tolower(tools::file_ext(f))
    if (ext %in% c("csv", "txt")) {
      data.table::fread(f, showProgress = FALSE)
    } else if (ext %in% c("parquet", "parq")) {
      as.data.table(arrow::read_parquet(f))
    } else {
      # 未対応拡張子はスキップ
      return(NULL)
    }
  }
  
  alist <- lapply(files, read_one)
  alist <- Filter(Negate(is.null), alist)
  if (length(alist) == 0L) {
    stop("Found files, but none had a supported extension (csv/txt/parquet).")
  }
  
  dt_all <- data.table::rbindlist(alist, use.names = TRUE, fill = TRUE)
  dt_all <- norm_alerts(dt_all)
  
  # ADM2×yearで合算（同一キー重複を吸収）
  alerts_year <- dt_all[, .(alerts = sum(alerts, na.rm = TRUE)),
                        by = .(adm2_code, year)]
  setorder(alerts_year, adm2_code, year)
  
  # 出力先を用意して保存（圧縮込み）
  dir.create(dirname(out_par), showWarnings = FALSE, recursive = TRUE)
  arrow::write_parquet(alerts_year, out_par, compression = "zstd")
  
  invisible(out_par)
}



# ========= CHIRPS（年次） =========
chirps <- fast_read_csv(file.path(root_raw, "idn_chirps_adm2_2019_2025.csv"))
setnames(chirps, tolower(names(chirps)))   # 例: adm1_nam, adm2_nam, calendar_year, rainy_season, year

# キー列を統一（ADM名は任意）
safe_rename(chirps, c("adm1_name","adm1_nam","adm1"), "adm1_name")
safe_rename(chirps, c("adm2_name","adm2_nam","adm2"), "adm2_name")
safe_rename(chirps, c("adm2_code","adm2id","adm2"), "adm2_code")
safe_rename(chirps, c("year","calendar_year_number","yr"), "year")

# 年合計 (= calendar_year_mm) を chirps_mm に、雨季合計を chirps_rainy_mm に
found_ann <- safe_rename(
  chirps,
  c("calendar_year_mm", "calendar_year", "chirps_mm", "chirps",
    "precip_annual", "annual_precip_mm", "total_precip_mm",
    "sum_mm", "ppt_mm", "precip_mm"),
  "chirps_mm"
)
if (!found_ann) {
  stop("CHIRPS: 年合計の列が見つかりません。現在の列名: ",
       paste(names(chirps), collapse = ", "))
}

# 雨季（任意）：rainy_season_mm を拾う
safe_rename(
  chirps,
  c("rainy_season_mm", "rainy_season", "wet_season_mm", "wet_season"),
  "chirps_rainy_mm"
)

# 余計な列を落として型を確定
keep <- c("adm1_name","adm2_code","adm2_name","year","chirps_mm","chirps_rainy_mm")
keep <- intersect(keep, names(chirps))
chirps <- chirps[, ..keep]

chirps[, `:=`(
  adm2_code = as.character(adm2_code),
  year      = as.integer(year),
  chirps_mm = as.numeric(chirps_mm)
)]
if ("chirps_rainy_mm" %in% names(chirps)) {
  chirps[, chirps_rainy_mm := as.numeric(chirps_rainy_mm)]
}

write_parquet_safely(chirps, file.path(out_dir, "chirps_year.parquet"))
message("✅ chirps_year.parquet written: ", nrow(chirps), " rows")



# ========= VIIRS NTL（年次） =========
viirs <- fast_read_csv(file.path(root_raw, "idn_viirs_ntl_adm2_2019_2025.csv"))
setnames(viirs, tolower(names(viirs)))
safe_rename(viirs, c("adm2_code","adm2id","adm2"), "adm2_code")
safe_rename(viirs, c("year","yr"), "year")
safe_rename(viirs, c("viirs_ntl","ntl","dn"), "viirs_ntl")
viirs <- viirs[, .(adm2_code = as.character(adm2_code),
                   year = as.integer(year),
                   viirs_ntl = as.numeric(viirs_ntl))]
write_parquet_safely(viirs, file.path(out_dir, "viirs_year.parquet"))
message("✅ viirs_year.parquet: ", nrow(viirs), " rows")

# ========= 最後に存在チェック =========
chk <- function(p) if (file.exists(p)) "OK" else "missing"
paths <- c(
  "data/interim/clouds_period.parquet",
  "data/interim/burned_period.parquet",
  "data/interim/forestloss_period.parquet",
  "data/interim/hotspot_period.parquet",
  "data/interim/alerts_year.parquet",
  "data/interim/chirps_year.parquet",
  "data/interim/viirs_year.parquet"
)
names(paths) <- c("clouds","burned","forest","hotspot","alerts","chirps","viirs")
print(vapply(paths, chk, character(1)))
# -----------------------------------------------


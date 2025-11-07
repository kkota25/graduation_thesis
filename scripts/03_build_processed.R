# scripts/03_build_processed.R
# ------------------------------------------------------------
# 目的: data/interim の中間成果を結合し、分析用パネルを parquet で保存
# 入力: data/interim/alerts_adm2_year.csv など
# 出力: data/processed/adm2_panel.parquet
# ------------------------------------------------------------
suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
  library(readr)
  library(here)
  library(fs)
})

build_processed <- function(){
  dir_create(here("data","processed"))
  
  # 1) alerts（必須）
  alerts <- readr::read_csv(here("data","interim","alerts_adm2_year.csv"), show_col_types = FALSE)
  setDT(alerts)
  
  # 2) 他の共変量があればここで読み込み・結合
  # 例:
  # clouds <- fread(here("data","interim","cloud_share_adm2_year.csv"))
  # chirps <- fread(here("data","interim","chirps_mm_adm2_year.csv"))
  # ntl    <- fread(here("data","interim","viirs_ntl_adm2_year.csv"))
  # burn   <- fread(here("data","interim","burn_area_adm2_year.csv"))
  
  panel <- copy(alerts)
  # 必須列の存在チェック
  stopifnot(all(c("adm2_code","year","alerts") %in% names(panel)))
  
  # 3) 型・並び
  panel[, year := as.integer(year)]
  setorder(panel, adm2_code, year)
  
  # 4) 保存
  out_parq <- here("data","processed","adm2_panel.parquet")
  arrow::write_parquet(panel, out_parq)
  message("Wrote: ", out_parq)
  invisible(panel[])
}

# scripts/21_build_processed.R
# ADM2 × year のパネルデータ（alerts + chirps + clouds + burned + forest loss）
# を作成して data/processed/adm2_panel.parquet に書き出す

source("R/utils_io.R")

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
})

#--------------------------------------------------
# ADM2 × year パネルを作成するメイン関数
#--------------------------------------------------

build_adm2_panel <- function(
    input_dir   = "data/interim",
    output_path = "data/processed/adm2_panel.parquet",
    year_min    = 2019,
    year_max    = 2024   # 必要に応じて 2025 に変更
) {
  
  message("=== build_adm2_panel: start ===")
  
  # ---------- 1. 中間データの読み込み ----------
  alerts <- arrow::read_parquet(file.path(input_dir, "alerts_year.parquet"))
  chirps <- arrow::read_parquet(file.path(input_dir, "chirps_year.parquet"))
  burned <- arrow::read_parquet(file.path(input_dir, "burned_period.parquet"))
  clouds <- arrow::read_parquet(file.path(input_dir, "clouds_year.parquet"))
  floss  <- arrow::read_parquet(file.path(input_dir, "forestloss_year.parquet"))
  
  lc_modis <- lc_modis |>
    filter(dplyr::between(year, year_min, year_max)) |>
    # panel 側と重複しそうな列は落としておく（ADM 名称など）
    select(
      -`system:index`,
      -.geo,
      -adm1_name,
      -adm2_name
    )
  
  # ---------- 2. 年の範囲でフィルタ ----------
  alerts <- alerts |> filter(dplyr::between(year, year_min, year_max))
  chirps <- chirps |> filter(dplyr::between(year, year_min, year_max))
  burned <- burned |> filter(dplyr::between(year, year_min, year_max))
  clouds <- clouds |> filter(dplyr::between(year, year_min, year_max))
  floss  <- floss  |> filter(dplyr::between(year, year_min, year_max))
  
  # ---------- 3. ADM2 × year で結合 ----------
  panel <- alerts |>
    arrange(adm2_code, year) |>
    # CHIRPS（降水）
    left_join(
      chirps |> select(adm2_code, year, chirps_mm, chirps_rainy_mm),
      by = c("adm2_code", "year")
    ) |>
    # 雲・晴天シェア
    left_join(
      clouds |> select(adm2_code, year, cloud_share, clear_share),
      by = c("adm2_code", "year")
    ) |>
    # 焼失面積
    left_join(
      burned |> select(adm2_code, year, burned_ha),
      by = c("adm2_code", "year")
    ) |>
    # 森林減少
    left_join(
      floss |> select(adm2_code, year, defor_rate, forest2000_ha, loss_ha),
      by = c("adm2_code", "year")
    ) |>
    # MODIS 土地被覆（ha_* / share_* / total_ha など全て）
    left_join(
      lc_modis,
      by = c("adm2_code", "year")
    ) |>
    arrange(adm2_code, year)
  
  # ---------- 4. ADM2×year が一意かチェック ----------
  n_unique <- dplyr::n_distinct(panel$adm2_code, panel$year)
  if (n_unique != nrow(panel)) {
    stop("adm2_code × year が一意ではありません（重複行があります）。")
  }
  
  # ---------- 5. 書き出し ----------
  dir.create(dirname(output_path), showWarnings = FALSE, recursive = TRUE)
  write_parquet_safely(panel, output_path)
  
  message("=== build_adm2_panel: written to ", output_path, " ===")
  
  invisible(panel)
}

#--------------------------------------------------
# スクリプトとして呼ばれたら必ず実行する部分
#--------------------------------------------------

panel_out <- build_adm2_panel()
print(dplyr::glimpse(panel_out))

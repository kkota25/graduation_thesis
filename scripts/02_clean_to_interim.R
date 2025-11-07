# scripts/02_clean_to_interim.R
# ------------------------------------------------------------
# 目的: GFW Data API から Integrated Deforestation Alerts を年別に取得し、
#       ADM2 で空間集計して data/interim/alerts_adm2_year.{csv,rds} を作成
# 依存: APIキー (環境変数 GFW_API_KEY)、ADM2境界（無ければ自動取得）
# 実行: Sys.setenv(GFW_API_KEY="..."); source("scripts/02_clean_to_interim.R");
#       alerts <- run_build_alerts_interim()
# 参考: APIエンドポイント/フィールド仕様（date/confidence/intensity）:contentReference[oaicite:2]{index=2}
# ------------------------------------------------------------

suppressPackageStartupMessages({
  library(httr)
  library(jsonlite)
  library(sf)
  library(terra)
  library(dplyr)
  library(data.table)
  library(readr)
  library(fs)
  library(here)
})

# ===== 設定 ===================================================
API_URL <- "https://data-api.globalforestwatch.org/dataset/gfw_integrated_alerts/latest/query/json"
TARGET_YEARS <- 2019:2025
CONF_FILTER  <- NULL      # 例: c("nominal","high") にすると信頼度でフィルタ
DEFAULT_ADM2_GPKG <- here("data","raw","adm","IDN_adm2.gpkg")
ADM2_CODE_CANDIDATES <- c("GID_2","ADM2_PCODE","adm2_code","ADM2_CODE")

# インドネシアのBBOX（WGS84）
IDN_BBOX <- list(
  type = "Polygon",
  coordinates = list(list(
    c(95, -11), c(141, -11), c(141, 6), c(95, 6), c(95, -11)
  ))
)

.dir_prep <- function(){
  dir_create(here("data","interim"))
  dir_create(here("data","raw","adm"))
  dir_create(here("outputs","logs"))
}

.ensure_adm2_gpkg <- function(adm2_path = DEFAULT_ADM2_GPKG){
  .dir_prep()
  if (file_exists(adm2_path)) return(adm2_path)
  if (!requireNamespace("geodata", quietly = TRUE)) {
    stop("ADM2境界がありません。install.packages('geodata') 実行後に再試行してください。")
  }
  v <- geodata::gadm(country = "IDN", level = 2, version = "4.1")
  terra::writeVector(v, adm2_path, filetype = "GPKG", overwrite = TRUE)
  if (!file_exists(adm2_path)) stop("ADM2境界の自動取得に失敗しました。")
  adm2_path
}

# ===== API呼び出し ============================================
.gfw_post <- function(sql, geometry){
  key <- Sys.getenv("GFW_API_KEY", "")
  if (identical(key, "")) stop("環境変数 GFW_API_KEY が未設定です。APIキーを設定してください。")
  body <- list(sql = sql, geometry = geometry)
  res <- httr::POST(
    url = API_URL,
    add_headers("x-api-key" = key, "Content-Type" = "application/json"),
    body = jsonlite::toJSON(body, auto_unbox = TRUE),
    timeout(120)
  )
  if (status_code(res) != 200) {
    stop("GFW API error: HTTP ", status_code(res), " | ", suppressWarnings(content(res, "text", encoding="UTF-8")))
  }
  ct <- content(res, as = "text", encoding = "UTF-8")
  js <- fromJSON(ct, simplifyVector = TRUE)
  if (is.null(js$data) || length(js$data) == 0) return(tibble::tibble())
  tibble::as_tibble(js$data)
}

.fetch_year_points <- function(year){
  d1 <- sprintf("%d-01-01", year)
  d2 <- sprintf("%d-12-31", year)
  where <- sprintf("gfw_integrated_alerts__date >= '%s' AND gfw_integrated_alerts__date <= '%s'", d1, d2)
  if (!is.null(CONF_FILTER) && length(CONF_FILTER) > 0) {
    cf <- paste(sprintf("'%s'", CONF_FILTER), collapse = ",")
    where <- sprintf("(%s) AND gfw_integrated_alerts__confidence IN (%s)", where, cf)
  }
  sql <- paste(
    "SELECT longitude, latitude, gfw_integrated_alerts__date",
    if(!is.null(CONF_FILTER)) ", gfw_integrated_alerts__confidence" else "",
    "FROM results WHERE", where
  )
  .gfw_post(sql, IDN_BBOX)
}

# ===== メイン =================================================
run_build_alerts_interim <- function(adm2_path = DEFAULT_ADM2_GPKG, years = TARGET_YEARS){
  .dir_prep()
  adm2_path <- .ensure_adm2_gpkg(adm2_path)
  
  # ADM2読み込みとコード列の統一
  adm2 <- sf::st_read(adm2_path, quiet = TRUE)
  code_col <- intersect(ADM2_CODE_CANDIDATES, names(adm2))
  if (length(code_col) == 0) stop("ADM2コード列が見つかりません。候補: ", paste(ADM2_CODE_CANDIDATES, collapse=", "))
  data.table::setDT(adm2); data.table::setnames(adm2, code_col[1], "adm2_code"); adm2 <- sf::st_as_sf(adm2)
  
  # 年ごとにAPI取得（サイズ対策）
  out <- vector("list", length(years))
  pb <- utils::txtProgressBar(min=0, max=length(years), style=3)
  for(i in seq_along(years)){
    yy <- years[i]
    dt <- .fetch_year_points(yy)
    if (nrow(dt)) dt$year <- yy
    out[[i]] <- dt
    utils::setTxtProgressBar(pb, i)
  }
  close(pb)
  
  pts <- data.table::rbindlist(out, use.names = TRUE, fill = TRUE)
  if (nrow(pts) == 0) {
    readr::write_lines(
      c("GFW APIからデータが返りませんでした。",
        "確認事項: (1) GFW_API_KEY 設定 (2) 年範囲とBBOX (3) confidenceフィルタが厳しすぎないか"),
      here("outputs","logs","02_api_empty_note.txt")
    )
    stop("API応答が空です。outputs/logs を参照して設定を見直してください。")
  }
  
  # sf化 → ADM2空間結合 → 年別カウント
  sf_pts <- sf::st_as_sf(pts, coords = c("longitude","latitude"), crs = 4326, remove = FALSE)
  # 必要に応じて投影を合わせる
  if(!is.na(sf::st_crs(adm2))) sf_pts <- sf::st_transform(sf_pts, sf::st_crs(adm2))
  
  joined <- sf::st_join(sf_pts, adm2["adm2_code"], left = TRUE)
  dt <- data.table::as.data.table(joined)
  dt <- dt[!is.na(adm2_code)]
  
  # 年別・ADM2別に件数を集計（強度で重み付けしたい場合は .N を sum(intensity) に置換）
  agg <- dt[, .(alerts = .N), by = .(adm2_code, year)]
  data.table::setorder(agg, adm2_code, year)
  
  out_csv <- here("data","interim","alerts_adm2_year.csv")
  out_rds <- here("data","interim","alerts_adm2_year.rds")
  readr::write_csv(agg, out_csv)
  saveRDS(agg, out_rds)
  
  message("Wrote: ", out_csv)
  invisible(agg[])
}



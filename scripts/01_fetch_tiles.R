# scripts/01_fetch_glad_radd.R
suppressPackageStartupMessages({
  library(data.table); library(httr); library(jsonlite); library(sf); library(here); library(arrow); library(fs)
})
dir_create(here("data","interim"))

# ---- 取得対象と期間 ----
date_min <- "2019-01-01"
date_max <- "2025-12-31"

# ---- RESTエンドポイント ----
END_GLAD <- "https://services2.arcgis.com/XXXX/arcgis/rest/services/GLAD_S2/FeatureServer/0/query"
END_RADD <- "https://services2.arcgis.com/XXXX/arcgis/rest/services/RADD/FeatureServer/0/query"


# ---- 必須フィールド名（レイヤ仕様に合わせて）----
FIELD_DATE   <- "event_date"    # ←要置換: 日付/発生日のフィールド
FIELD_CONF   <- "confidence"    # ←要置換: 信頼度（無いなら NULL）
FIELD_X      <- "longitude"     # ←要置換: 経度（無ければ geometry を使う）
FIELD_Y      <- "latitude"      # ←要置換: 緯度（無ければ geometry を使う）
FIELD_ADM2   <- "ADM2_CODE"     # ←要置換: 既に属性にADM2コードがあればここを設定。無ければ NA のまま

# ---- 共通: ArcGIS REST からページング取得 ----
fetch_all <- function(base_url, where_extra="1=1"){
  out <- list(); off <- 0L; i <- 0L
  repeat{
    q <- list(
      where = sprintf("(%s) AND %s >= DATE '%s' AND %s <= DATE '%s'",
                      where_extra, FIELD_DATE, date_min, FIELD_DATE, date_max),
      outFields = "*",
      f = "json",
      returnGeometry = "true",
      resultRecordCount = 2000,
      resultOffset = off,
      outSR = 4326
    )
    r <- RETRY("GET", base_url, query = q, times = 5,
               pause_min = 1, pause_cap = 8, terminate_on = c(400,401,403,404),
               timeout(60))
    stop_for_status(r)
    
    js <- content(r, as="text", encoding="UTF-8"); m <- fromJSON(js, simplifyVector = FALSE)
    feats <- m$features; if (length(feats)==0) break
    attr <- lapply(feats, `[[`, "attributes")
    geom <- lapply(feats, `[[`, "geometry")
    dt   <- rbindlist(lapply(seq_along(attr), function(k){
      a <- attr[[k]]; g <- geom[[k]]
      data.table::as.data.table(c(a, g))
    }), fill = TRUE)
    i <- i + 1L; out[[i]] <- dt
    if (!isTRUE(m$exceededTransferLimit)) break
    off <- off + 2000L
  }
  if (!length(out)) return(data.table())
  rbindlist(out, use.names = TRUE, fill = TRUE)
}

# ---- GLAD と RADD を取得 ----
glad <- fetch_all(END_GLAD)
radd <- fetch_all(END_RADD)

# ---- 前処理：日付・座標・信頼度 ----
norm_alerts <- function(dt){
  if (!nrow(dt)) return(data.table())
  setDT(dt)
  # 日付
  setnames(dt, old = intersect(names(dt), FIELD_DATE), new = "date", skip_absent = TRUE)
  if (!"date" %in% names(dt)) stop("日付フィールドが見つからない")
  dt[, date := as.Date(substr(as.character(date), 1, 10))]
  
  # ADM2コード（属性にあれば直採用）
  if (!is.null(FIELD_ADM2) && FIELD_ADM2 %in% names(dt)) {
    setnames(dt, FIELD_ADM2, "adm2_code"); dt[, adm2_code := as.character(adm2_code)]
  } else {
    dt[, adm2_code := NA_character_]
  }
  
  # 信頼度（なければ全件採用）
  if (!is.null(FIELD_CONF) && FIELD_CONF %in% names(dt)) {
    setnames(dt, FIELD_CONF, "conf"); dt <- dt[is.na(conf) | conf >= 0]  # 閾値は必要なら調整
  }
  
  # 座標→sf（属性にlon/latが無ければ geometry.x/geometry.y を使用）
  guess_x <- if (FIELD_X %in% names(dt)) FIELD_X else "x"
  guess_y <- if (FIELD_Y %in% names(dt)) FIELD_Y else "y"
  if (!(guess_x %in% names(dt) && guess_y %in% names(dt))) {
    # ArcGIS geometry {x,y} が dt$x, dt$y で来ている想定
    if (!all(c("x","y") %in% names(dt))) warning("座標が見つからない。空間結合をスキップ")
  }
  
  dt[]
}

glad <- norm_alerts(glad)
radd <- norm_alerts(radd)

# ---- ADM2 付与：属性にADM2が無い場合のみ空間結合 ----
need_spjoin <- (any(is.na(glad$adm2_code)) || any(is.na(radd$adm2_code)))

if (need_spjoin) {
  # ADM2ポリゴンのパス
  ADM2_SHP <- here("data","raw","gaul2015_adm2","idn_adm2.shp")  # ←要置換: ADM2シェープファイルの実体
  if (!file.exists(ADM2_SHP)) stop("ADM2シェープが見つからない: ", ADM2_SHP)
  adm2 <- st_read(ADM2_SHP, quiet = TRUE) |> st_make_valid() |> st_transform(4326)
  key_nm <- intersect(names(adm2), c("ADM2_CODE","adm2_code","ADM2_PCODE"))
  if (!length(key_nm)) stop("ADM2コード列が見つからない")
  setnames(adm2, key_nm[1], "adm2_code"); adm2$adm2_code <- as.character(adm2$adm2_code)
  
  to_sf_pts <- function(dt){
    if (!all(c("x","y") %in% names(dt))) stop("x,yが無いので点化できない")
    st_as_sf(dt, coords = c("x","y"), crs = 4326, remove = FALSE)
  }
  
  if (any(is.na(glad$adm2_code))) {
    gsf <- to_sf_pts(glad)
    glad <- st_join(gsf, adm2["adm2_code"], left = TRUE) |> st_drop_geometry() |> as.data.table()
  }
  if (any(is.na(radd$adm2_code))) {
    rsf <- to_sf_pts(radd)
    radd <- st_join(rsf, adm2["adm2_code"], left = TRUE) |> st_drop_geometry() |> as.data.table()
  }
}

# ---- 年集計＋統合 ----
glad[, year := as.integer(format(date, "%Y"))]
radd[, year := as.integer(format(date, "%Y"))]
glad_y <- glad[!is.na(adm2_code) & !is.na(year), .(glad_n = .N), by=.(adm2_code, year)]
radd_y <- radd[!is.na(adm2_code) & !is.na(year), .(radd_n = .N), by=.(adm2_code, year)]

al <- merge(glad_y, radd_y, by=c("adm2_code","year"), all=TRUE)
al[is.na(glad_n), glad_n := 0L]
al[is.na(radd_n), radd_n := 0L]

# 件数足し合わせ or 論理和（どちらでも可）。ここでは足し合わせを alerts とする
al[, alerts := as.integer(glad_n + radd_n)]

setorder(al, adm2_code, year)

# 保存
out_par <- here("data","interim","alerts_year.parquet")
arrow::write_parquet(al[, .(adm2_code = as.character(adm2_code),
                            year = as.integer(year),
                            alerts = as.integer(alerts))],
                     out_par, compression = "zstd")
message("✅ wrote: ", out_par)


# scripts/01_fetch_glad_radd.R
# GLAD-S2 / RADD のイベントを ArcGIS REST から取得し、ADM2×year 件数を保存
# 出力: data/interim/alerts_year.parquet

suppressPackageStartupMessages({
  library(data.table); library(httr); library(jsonlite); library(sf)
  library(here); library(arrow); library(fs); library(dplyr)
})

project_renviron <- here(".Renviron")
if (file.exists(project_renviron)) {
  readRenviron(project_renviron)
}

# ========================= ユーザー設定（必ず確認） =========================
# A) イベントFeatureServerのベースURL（末尾は /FeatureServer）
#    ※「Integrated_deforestation_alerts」はタイル一覧なので使わない
get_arcgis_base <- function(env_var, dataset_label){
  val <- Sys.getenv(env_var, unset = "")
  if (!nzchar(val) || grepl("<org>", val, fixed = TRUE)) {
    if (interactive()) {
      message(sprintf("%s のイベントレイヤURLが未設定です。", dataset_label))
      message("例: https://<org>.maps.arcgis.com/.../FeatureServer")
      entered <- trimws(readline(prompt = sprintf("%s を入力してください（空欄で中止）: ", env_var)))
      if (nzchar(entered)) val <- entered
    }
  }
  if (!nzchar(val) || grepl("<org>", val, fixed = TRUE)) {
    stop(sprintf("%s が未設定です。環境変数 %s または .Renviron でイベントレイヤのベースURLを指定してください。",
                 env_var, env_var))
  }
  val
}

END_GLAD_BASE <- get_arcgis_base("END_GLAD_BASE", "GLAD")
END_RADD_BASE <- get_arcgis_base("END_RADD_BASE", "RADD")

# B) 統合レイヤを敢えて使う場合のフィルタ（通常は空でOK）
GLAD_WHERE <- ""  # 例: "source = 'GLAD-S2'"
RADD_WHERE <- ""  # 例: "source = 'RADD'"

# C) 期間
DATE_MIN  <- "2019-01-01"
DATE_MAX  <- "2025-12-31"

# D) フィールド名の手動指定（不明なら NULL のままで自動推定に任せる）
#    環境変数 FIELD_DATE_MANUAL / FIELD_DATE_FALLBACK でも指定可能
FIELD_DATE_MANUAL <- local({
  manual <- Sys.getenv("FIELD_DATE_MANUAL", unset = NA_character_)
  if (is.na(manual) || manual == "") NULL else manual
})
FIELD_ADM2_MANUAL <- NULL     # 例: "ADM2_CODE"（属性にADM2があれば）
FIELD_CONF_MANUAL <- NULL     # 例: "confidence"（使わないなら NULL）

# E) ADM2シェープ（属性にADM2が無い場合のみ使用）
ADM2_SHP <- here("data","raw","gaul2015_adm2","idn_adm2.shp")  # ←実体に合わせて要置換

# F) 日付フィルタの適用と句
USE_DATE_FILTER <- TRUE          # 取れない場合は FALSE にして原因切り分け
DATE_CLAUSE     <- "AUTO"        # "AUTO" / "DATE" / "TIMESTAMP"
AUTO_RETRY_NO_FILTER <- TRUE     # 0件時に日付フィルタを自動解除して再試行
# ============================================================================

ARCGIS_TOKEN <- Sys.getenv("ARCGIS_TOKEN", unset = "")
add_token <- function(query_list){
  if (!nzchar(ARCGIS_TOKEN)) return(query_list)
  c(query_list, list(token = ARCGIS_TOKEN))
}
# ============================================================================
dir_create(here("data","interim"))

# ---- ユーティリティ ----
layer_has_date <- function(layer_query_url){
  meta <- httr::content(
    httr::GET(sub("/query$","", layer_query_url), query=add_token(list(f="json"))),
    as="parsed", type="application/json"
  )
  if (is.null(meta$fields)) return(FALSE)
  tp <- vapply(meta$fields, `[[`, "", "type")
  any(tp == "esriFieldTypeDate")
}

pick_layer <- function(base_url){
  for (i in 0:9) {
    qurl <- sprintf("%s/%d/query", base_url, i)
    q <- add_token(list(where="1=1", outFields="OBJECTID", returnGeometry="false",
                        resultRecordCount=1, f="json"))
    message(sprintf("layer %d にアクセス: %s", i, qurl))
    r <- httr::RETRY("GET", qurl,
                     query = q,
                     times=3, pause_min=1, pause_cap=4, timeout(30))
    message(sprintf("layer %d レスポンスコード: %s", i, httr::status_code(r)))
    if (httr::http_error(r)) {
      message(sprintf("layer %d で HTTP エラーのためスキップ: %s", i, qurl))
      next
    }
    m <- httr::content(r, as="parsed", type="application/json")
    if (!is.null(m$error)) {
      code <- m$error$code
      err_msg <- if (!is.null(m$error$message)) m$error$message else ""
      err_detail <- if (!is.null(m$error$details)) paste(m$error$details, collapse=" | ") else ""
      message(sprintf("layer %d エラー: code=%s message=%s details=%s", i, code, err_msg, err_detail))
      if (code %in% c(498,499)) stop("トークン必須レイヤ：", base_url, "/", i)
      next
    }
    if (isTRUE(layer_has_date(qurl))) {
      message(sprintf("layer %d を採用: %s", i, qurl))
      return(qurl)
    }
  }
  stop(
    "有効なレイヤが見つかりません: ", base_url, "\n",
    "注意: Integrated_deforestation_alerts はタイル一覧（date無し）です。イベントレイヤを指定してください。"
  )
}

get_meta <- function(query_url){
  base <- sub("/query$", "", query_url)
  r <- RETRY("GET", base, query=add_token(list(f="json")), times=3, pause_min=1, pause_cap=4, timeout(30))
  stop_for_status(r)
  content(r, as="parsed", type="application/json")
}

guess_date_field <- function(meta, manual=NULL){
  if (!is.null(manual)) return(manual)
  f <- meta$fields; if (is.null(f)) return(NULL)
  nm <- vapply(f, `[[`, "", "name")
  tp <- vapply(f, `[[`, "", "type")
  cand <- nm[tp == "esriFieldTypeDate"]
  if (length(cand)) cand[1] else NULL
}

resolve_date_field <- function(meta, manual=NULL){
  guessed <- guess_date_field(meta, manual)
  if (!is.null(guessed)) return(guessed)
  
  fields <- meta$fields
  field_names <- if (is.null(fields)) character() else vapply(fields, `[[`, "", "name")
  manual_env <- Sys.getenv("FIELD_DATE_FALLBACK", unset = "")
  if (nzchar(manual_env)) {
    message(sprintf("環境変数 FIELD_DATE_FALLBACK を日付列として使用します: %s", manual_env))
    return(manual_env)
  }
  
  if (interactive()) {
    message("日付フィールドを推定できませんでした。候補: ", paste(field_names, collapse=", "))
    manual_input <- trimws(readline(prompt = "日付フィールド名を入力してください（空欄で中止）: "))
    if (nzchar(manual_input)) return(manual_input)
  }
  
  stop("日付フィールドを決定できませんでした。候補: ", paste(field_names, collapse=", "),
       "\n環境変数 FIELD_DATE_FALLBACK または FIELD_DATE_MANUAL を設定してください。")
}

build_where <- function(where_extra, date_field, query_url){
  if (!USE_DATE_FILTER || is.null(date_field)) return(where_extra)
  
  if (DATE_CLAUSE %in% c("DATE","TIMESTAMP")) {
    return(
      switch(DATE_CLAUSE,
             DATE = sprintf("(%s) AND %s >= DATE '%s' AND %s < DATE '%s'",
                            where_extra, date_field, DATE_MIN, date_field, as.character(as.Date(DATE_MAX)+1)),
             TIMESTAMP = sprintf("(%s) AND %s >= TIMESTAMP '%s 00:00:00' AND %s < TIMESTAMP '%s 00:00:00'",
                                 where_extra, date_field, DATE_MIN, date_field, as.character(as.Date(DATE_MAX)+1))
      )
    )
  }
  
  try_patterns <- list(
    sprintf("%s >= DATE '%s' AND %s < DATE '%s'", date_field, DATE_MIN, date_field, as.character(as.Date(DATE_MAX)+1)),
    sprintf("%s >= TIMESTAMP '%s 00:00:00' AND %s < TIMESTAMP '%s 00:00:00'", date_field, DATE_MIN, date_field, as.character(as.Date(DATE_MAX)+1))
  )
  for (pat in try_patterns) {
    q <- add_token(list(where = pat, outFields="OBJECTID", returnGeometry="false", resultRecordCount=1, f="json"))
    r <- RETRY("GET", query_url, query=q, times=3, pause_min=1, pause_cap=4, timeout(30))
    if (!http_error(r)) {
      m <- content(r, as="parsed", type="application/json")
      if (!is.null(m$features)) return(sprintf("(%s) AND %s", where_extra, pat))
    }
  }
  where_extra
}

fetch_all <- function(query_url, where_extra="1=1", date_field=NULL, auto_retry_no_filter=AUTO_RETRY_NO_FILTER){
  attempt_fetch <- function(){
    where_final <- build_where(where_extra, date_field, query_url)
    out <- list(); off <- 0L; page <- 0L; total <- 0L
    repeat{
      q <- add_token(list(where=where_final, outFields="*", f="json", returnGeometry="true",
                          resultRecordCount=2000, resultOffset=off, outSR=4326))
      r <- RETRY("GET", query_url, query=q, times=5, pause_min=1, pause_cap=8,
                 terminate_on=c(400,401,403,404), timeout(60))
      stop_for_status(r)
      m <- content(r, as="parsed", type="application/json")
      if (!is.null(m$error)) stop(sprintf("ArcGISエラー %s: %s", m$error$code, m$error$message))
      feats <- m$features; page <- page + 1L; n <- length(feats)
      message(sprintf("GET page=%d offset=%d n=%d exceeded=%s", page, off, n, as.character(isTRUE(m$exceededTransferLimit))))
      if (n==0) break
      attr <- lapply(feats, `[[`, "attributes")
      geom <- lapply(feats, `[[`, "geometry")
      dt   <- rbindlist(lapply(seq_along(attr), function(k){ as.data.table(c(attr[[k]], geom[[k]])) }), fill=TRUE)
      out[[length(out)+1L]] <- dt; total <- total + n
      if (!isTRUE(m$exceededTransferLimit)) break
      off <- off + 2000L
    }
    message(sprintf("合計取得件数: %d", total))
    list(
      data = if (!length(out)) data.table() else rbindlist(out, use.names=TRUE, fill=TRUE),
      where = where_final,
      total = total
    )
  }
  
  res <- attempt_fetch()
  if (!nrow(res$data) && auto_retry_no_filter && USE_DATE_FILTER) {
    message("0件のため USE_DATE_FILTER を FALSE にして再試行します。")
    old <- USE_DATE_FILTER
    USE_DATE_FILTER <<- FALSE
    on.exit({ USE_DATE_FILTER <<- old }, add = TRUE)
    res <- attempt_fetch()
  }
  
  dt <- res$data
  if (!nrow(dt)) {
    msg <- sprintf("レイヤからデータが取得できませんでした: %s\nWHERE: %s", query_url, res$where)
    message(msg)
    stop(msg)
  }
  
  date_summary <- NA_character_
  if (!is.null(date_field) && date_field %in% names(dt)) {
    parsed <- parse_date_any(dt[[date_field]])
    if (all(is.na(parsed))) {
      date_summary <- "日付解釈不可"
    } else {
      date_summary <- sprintf("%s 〜 %s",
                              format(min(parsed, na.rm=TRUE), "%Y-%m-%d"),
                              format(max(parsed, na.rm=TRUE), "%Y-%m-%d"))
    }
  }
  message(sprintf("取得サマリ: url=%s 件数=%d 日付範囲=%s",
                  query_url, nrow(dt), ifelse(is.na(date_summary), "不明", date_summary)))
  dt
}

parse_date_any <- function(x){
  if (is.numeric(x)) {
    if (suppressWarnings(max(x, na.rm=TRUE)) > 1e12) as.POSIXct(x/1000, origin="1970-01-01", tz="UTC")
    else as.POSIXct(x, origin="1970-01-01", tz="UTC")
  } else {
    as.POSIXct(substr(as.character(x),1,19), tz="UTC")
  }
}

norm_alerts <- function(dt, field_date, field_adm2=NULL, field_conf=NULL){
  if (!nrow(dt)) stop("ダウンロード結果が空です。")
  setDT(dt)
  
  # 日付列の決定（手動指定 > 典型名 > パターン）
  cand <- character(0)
  if (!is.null(field_date) && field_date %in% names(dt)) cand <- field_date
  if (!length(cand)) {
    prefer <- c("event_date","acq_date","alert_date","first_date","detect_date","obs_date","date","timestamp","time")
    present <- intersect(prefer, names(dt))
    if (length(present)) cand <- present[1]
  }
  if (!length(cand)) {
    any_dt <- grep("date|time", names(dt), ignore.case=TRUE, value=TRUE)
    if (length(any_dt)) cand <- any_dt[1]
  }
  if (!length(cand)) stop("日付フィールドが見つかりません。列名: ", paste(names(dt), collapse=", "))
  setnames(dt, cand, "date_raw")
  dt[, date := as.Date(parse_date_any(date_raw))]
  
  # ADM2コード
  if (!is.null(field_adm2) && field_adm2 %in% names(dt)) {
    setnames(dt, field_adm2, "adm2_code"); dt[, adm2_code := as.character(adm2_code)]
  } else if (!"adm2_code" %in% names(dt)) {
    dt[, adm2_code := NA_character_]
  }
  
  # 信頼度（任意）
  if (!is.null(field_conf) && field_conf %in% names(dt)) {
    setnames(dt, field_conf, "conf")
    dt <- dt[is.na(conf) | conf >= 0]
  }
  
  message(sprintf("採用した日付列: %s → date に正規化", cand))
  dt[]
}

# ---- イベントレイヤ解決 ----
END_GLAD <- pick_layer(END_GLAD_BASE)
if (grepl("Integrated_deforestation_alerts", END_GLAD, fixed=TRUE)) {
  stop("GLADのURLがタイル一覧です（date無し）: ", END_GLAD)
}
END_RADD <- pick_layer(END_RADD_BASE)
if (grepl("Integrated_deforestation_alerts", END_RADD, fixed=TRUE)) {
  stop("RADDのURLがタイル一覧です（date無し）: ", END_RADD)
}

# ---- メタから日付列候補 ----
meta_g <- get_meta(END_GLAD); meta_r <- get_meta(END_RADD)
FIELD_DATE_G <- resolve_date_field(meta_g, FIELD_DATE_MANUAL)
FIELD_DATE_R <- resolve_date_field(meta_r, FIELD_DATE_MANUAL)
message(sprintf("GLAD 日付フィールド: %s", FIELD_DATE_G))
message(sprintf("RADD 日付フィールド: %s", FIELD_DATE_R))
FIELD_ADM2_G <- FIELD_ADM2_MANUAL; FIELD_ADM2_R <- FIELD_ADM2_MANUAL
FIELD_CONF_G <- FIELD_CONF_MANUAL; FIELD_CONF_R <- FIELD_CONF_MANUAL

# ---- ダウンロード ----
glad <- fetch_all(END_GLAD, where_extra = if(nzchar(GLAD_WHERE)) GLAD_WHERE else "1=1",
                  date_field = FIELD_DATE_G)
radd <- fetch_all(END_RADD, where_extra = if(nzchar(RADD_WHERE)) RADD_WHERE else "1=1",
                  date_field = FIELD_DATE_R)

# ---- 正規化 ----
glad <- norm_alerts(glad, FIELD_DATE_G, FIELD_ADM2_G, FIELD_CONF_G)
radd <- norm_alerts(radd, FIELD_DATE_R, FIELD_ADM2_R, FIELD_CONF_R)

# ---- ADM2付与（必要時のみ空間結合）----
need_spjoin <- any(is.na(glad$adm2_code)) || any(is.na(radd$adm2_code))
if (need_spjoin) {
  if (!file.exists(ADM2_SHP)) stop("ADM2シェープが見つかりません: ", ADM2_SHP)
  adm2 <- st_read(ADM2_SHP, quiet=TRUE) |> st_make_valid() |> st_transform(4326)
  key_nm <- intersect(names(adm2), c("ADM2_CODE","adm2_code","ADM2_PCODE"))
  if (!length(key_nm)) stop("ADM2コード列がADM2シェープに見つかりません")
  names(adm2)[match(key_nm[1], names(adm2))] <- "adm2_code"; adm2$adm2_code <- as.character(adm2$adm2_code)
  
  to_sf_pts <- function(dt){
    if (!all(c("x","y") %in% names(dt))) stop("点座標 x,y が見つかりません。属性にADM2があるなら FIELD_ADM2_MANUAL を設定してください。")
    st_as_sf(dt, coords=c("x","y"), crs=4326, remove=FALSE)
  }
  if (any(is.na(glad$adm2_code))) {
    glad <- st_join(to_sf_pts(glad), adm2["adm2_code"], left=TRUE) |> st_drop_geometry() |> as.data.table()
  }
  if (any(is.na(radd$adm2_code))) {
    radd <- st_join(to_sf_pts(radd), adm2["adm2_code"], left=TRUE) |> st_drop_geometry() |> as.data.table()
  }
}

# ---- 年集計 → 結合 ----
glad[, year := as.integer(format(date, "%Y"))]
radd[, year := as.integer(format(date, "%Y"))]
glad_y <- glad[!is.na(adm2_code) & !is.na(year), .(glad_n=.N), by=.(adm2_code, year)]
radd_y <- radd[!is.na(adm2_code) & !is.na(year), .(radd_n=.N), by=.(adm2_code, year)]

al <- merge(glad_y, radd_y, by=c("adm2_code","year"), all=TRUE)
al[is.na(glad_n), glad_n := 0L][is.na(radd_n), radd_n := 0L][, alerts := as.integer(glad_n + radd_n)]
al <- al[year >= as.integer(substr(DATE_MIN,1,4)) & year <= as.integer(substr(DATE_MAX,1,4))]
setorder(al, adm2_code, year)

# ---- 保存 + QC ----
out_par <- here("data","interim","alerts_year.parquet")
arrow::write_parquet(al[, .(adm2_code=as.character(adm2_code), year=as.integer(year), alerts=as.integer(alerts))],
                     out_par, compression="zstd")
message("✅ wrote: ", out_par)

qc <- al[, .(n_adm2=.N, alerts_sum=sum(alerts)), by=year][order(year)]
print(qc)


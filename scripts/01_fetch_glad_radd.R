# scripts/01_fetch_glad_radd.R
# ------------------------------------------------------------
# 目的: GFW "Integrated deforestation alerts" のタイル索引をAPIから取得し、
#       tile_idごとのGeoTIFFをローカルに保存する
# 参照: ArcGIS FeatureServer layer 0 (fields: tile_id, download; MaxRecordCount=2000)
# 出力:
#   - outputs/logs/gfw_tile_index.csv  … タイル索引ログ
#   - outputs/tiles/gfw_tiles/*.tif    … 取得したGeoTIFF
# 使い方（R Console または Rmdのchunkで）:
#   library(here); source(here("scripts","01_fetch_glad_radd.R"))
#   idx <- run_fetch_alerts()
# ------------------------------------------------------------

suppressPackageStartupMessages({
  library(httr)
  library(jsonlite)
  library(fs)
  library(readr)
  library(dplyr)
  library(here)
})

# ---- 設定 ---------------------------------------------------
.arc_base <- "https://services2.arcgis.com/g8WusZB13b9OegfU/arcgis/rest/services/Integrated_deforestation_alerts/FeatureServer/0/query"

# 既定はインドネシア全域のBBOX（WGS84）
# xmin=95, ymin=-11, xmax=141, ymax=6
.default_bbox <- c(95, -11, 141, 6)

# UA（ダウンロード安定化）
.ua <- httr::user_agent("R/httr GFW-integrated-alerts fetcher (contact: none)")

# ---- ユーティリティ -----------------------------------------
.safe_json_get <- function(base, query, tries = 5, pause = 1){
  for(i in seq_len(tries)){
    resp <- try(httr::GET(url = base, query = query, .ua), silent = TRUE)
    if(inherits(resp, "response") && httr::status_code(resp) == 200){
      txt <- httr::content(resp, as = "text", encoding = "UTF-8")
      return(jsonlite::fromJSON(txt, simplifyVector = TRUE))
    }
    Sys.sleep(pause * i)
  }
  stop("GET failed: ", base)
}

.dir_prep <- function(){
  dir_create(here("outputs","logs"))
  dir_create(here("outputs","tiles","gfw_tiles"))
}

# ---- 1) タイル索引の取得 ------------------------------------
fetch_tile_index <- function(bbox = .default_bbox, page_size = 2000){
  stopifnot(length(bbox) == 4)
  off <- 0L
  out <- list()
  
  repeat{
    q <- list(
      where          = "1=1",
      geometry       = jsonlite::toJSON(
        list(xmin = bbox[1], ymin = bbox[2], xmax = bbox[3], ymax = bbox[4]),
        auto_unbox = TRUE
      ),
      geometryType   = "esriGeometryEnvelope",
      inSR           = 4326,                          # WGS84 指定
      spatialRel     = "esriSpatialRelIntersects",
      outFields      = "tile_id,download",
      returnGeometry = "false",
      resultRecordCount = page_size,
      resultOffset      = off,
      f              = "json"
    )
    m <- .safe_json_get(.arc_base, q)
    nrec <- length(m$features)
    if(nrec == 0) break
    
    # attributes だけを抽出
    df <- tibble::as_tibble(m$features$attributes) |>
      select(tile_id, download)
    
    out[[length(out) + 1L]] <- df
    off <- off + nrec
  }
  
  if(length(out) == 0) {
    tibble::tibble(tile_id = character(), download = character())
  } else {
    bind_rows(out) |>
      distinct(tile_id, .keep_all = TRUE)
  }
}

# ---- 2) タイルのダウンロード --------------------------------
download_tiles <- function(index_df,
                           outdir = here("outputs","tiles","gfw_tiles"),
                           retries = 3){
  stopifnot(all(c("tile_id","download") %in% names(index_df)))
  dir_create(outdir)
  
  # 進捗バー
  pb <- utils::txtProgressBar(min = 0, max = nrow(index_df), style = 3)
  on.exit(close(pb), add = TRUE)
  
  results <- vector("list", nrow(index_df))
  
  for(i in seq_len(nrow(index_df))){
    tile <- index_df$tile_id[i]
    href <- index_df$download[i]
    fout <- file.path(outdir, paste0(tile, ".tif"))
    
    ok <- FALSE
    if(file_exists(fout) && file_info(fout)$size > 0){
      ok <- TRUE
    } else {
      tries <- 0L
      while(!ok && tries < retries){
        tries <- tries + 1L
        # 一時ファイルに落としてからmoveで原子更新
        tmp <- tempfile(fileext = ".tif")
        res <- try(httr::GET(href, .ua, write_disk(tmp, overwrite = TRUE), timeout(120)),
                   silent = TRUE)
        if(inherits(res, "response") && status_code(res) == 200 && file_exists(tmp) && file_info(tmp)$size > 0){
          file_copy(tmp, fout, overwrite = TRUE)
          ok <- TRUE
        } else {
          Sys.sleep(1.5 * tries)
        }
        if(file_exists(tmp)) file_delete(tmp)
      }
    }
    
    results[[i]] <- tibble::tibble(tile_id = tile,
                                   path    = fout,
                                   ok      = ok,
                                   size    = ifelse(file_exists(fout), file_info(fout)$size, NA_real_))
    utils::setTxtProgressBar(pb, i)
  }
  bind_rows(results)
}

# ---- 3) 全体ラッパ -------------------------------------------
run_fetch_alerts <- function(bbox = .default_bbox){
  message("Checking service and preparing directories...")
  .dir_prep()
  
  message("Fetching tile index...")
  idx <- fetch_tile_index(bbox = bbox, page_size = 2000)
  
  if(nrow(idx) == 0){
    warning("インデックスが空です。BBOXが狭すぎる可能性があります。")
    # それでもログは出す
    write_csv(idx, here("outputs","logs","gfw_tile_index.csv"))
    return(invisible(idx))
  }
  
  # ログ保存
  write_csv(idx, here("outputs","logs","gfw_tile_index.csv"))
  
  message("Downloading tiles...")
  dl <- download_tiles(idx)
  
  # 結果まとめ
  merged <- idx |>
    left_join(dl, by = "tile_id") |>
    mutate(path = ifelse(is.na(path), file.path(here("outputs","tiles","gfw_tiles"), paste0(tile_id, ".tif")), path))
  
  n_ok   <- sum(merged$ok, na.rm = TRUE)
  n_fail <- sum(!merged$ok | is.na(merged$ok))
  
  message(sprintf("Done. %d downloaded / %d failed. Files at: %s",
                  n_ok, n_fail, here("outputs","tiles","gfw_tiles")))
  
  invisible(merged)
}


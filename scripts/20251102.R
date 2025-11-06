# GeoTIFF取得用コード
```{r GeoTIFF}
library(httr); library(jsonlite)
library(here)

base <- "https://services2.arcgis.com/g8WusZB13b9OegfU/arcgis/rest/services/Integrated_deforestation_alerts/FeatureServer/0/query"

# ここでは % を含む部分を固定文字列にしておき、
# 可変なのは最後の resultOffset だけを連結する
q_head <- paste0(
  "?where=1%3D1",
  "&geometry=%7B%22xmin%22%3A95%2C%22ymin%22%3A-11%2C%22xmax%22%3A141%2C%22ymax%22%3A6%7D",
  "&geometryType=esriGeometryEnvelope&inSR=4326&spatialRel=esriSpatialRelIntersects",
  "&outFields=tile_id,download&returnGeometry=false&resultRecordCount=2000",
  "&resultOffset="
)

# ループの前に
out_dir <- here::here("outputs","tiles","gfw")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)


off <- 0; n_total <- 0
repeat {
  url <- paste0(base, q_head, off, "&f=pjson")  # ← ここだけ差し替え
  js  <- content(GET(url), as="text", encoding="UTF-8")
  m   <- fromJSON(js)
  if (length(m$features) == 0) break
  
  for (i in seq_len(nrow(m$features))) {
    tile <- m$features$attributes$tile_id[i]
    href <- m$features$attributes$download[i]
    fn <- file.path(out_dir, paste0(tile, ".tif"))
    if (!file.exists(fn)) {
      cat(sprintf("[%s] downloading...\n", tile))
      ok <- FALSE; tries <- 0
      while(!ok && tries < 3){
        tries <- tries + 1
        try({
          download.file(href, fn, mode="wb", quiet=TRUE)
          ok <- file.exists(fn) && file.info(fn)$size > 0
        }, silent=TRUE)
      }
      if (!ok) cat(sprintf("  -> FAILED: %s\n", tile))
    }
    n_total <- n_total + 1
  }
  if (!isTRUE(m$exceededTransferLimit)) break
  off <- off + 2000
}

cat(sprintf("Done. tiles listed: %d  saved to %s\n",
            n_total, fs::path_rel(out_dir)))
```


# GeoTIFF取得用元コード
```{r GeoTIFF}
library(httr); library(jsonlite)

base <- "https://services2.arcgis.com/g8WusZB13b9OegfU/arcgis/rest/services/Integrated_deforestation_alerts/FeatureServer/0/query"

# ここでは % を含む部分を固定文字列にしておき、
# 可変なのは最後の resultOffset だけを連結する
q_head <- paste0(
  "?where=1%3D1",
  "&geometry=%7B%22xmin%22%3A95%2C%22ymin%22%3A-11%2C%22xmax%22%3A141%2C%22ymax%22%3A6%7D",
  "&geometryType=esriGeometryEnvelope&inSR=4326&spatialRel=esriSpatialRelIntersects",
  "&outFields=tile_id,download&returnGeometry=false&resultRecordCount=2000",
  "&resultOffset="
)

dir.create("gfw_tiles", showWarnings = FALSE)

off <- 0; n_total <- 0
repeat {
  url <- paste0(base, q_head, off, "&f=pjson")  # ← ここだけ差し替え
  js  <- content(GET(url), as="text", encoding="UTF-8")
  m   <- fromJSON(js)
  if (length(m$features) == 0) break
  
  for (i in seq_len(nrow(m$features))) {
    tile <- m$features$attributes$tile_id[i]
    href <- m$features$attributes$download[i]
    fn   <- file.path("gfw_tiles", paste0(tile, ".tif"))
    if (!file.exists(fn)) {
      cat(sprintf("[%s] downloading...\n", tile))
      ok <- FALSE; tries <- 0
      while(!ok && tries < 3){
        tries <- tries + 1
        try({
          download.file(href, fn, mode="wb", quiet=TRUE)
          ok <- file.exists(fn) && file.info(fn)$size > 0
        }, silent=TRUE)
      }
      if (!ok) cat(sprintf("  -> FAILED: %s\n", tile))
    }
    n_total <- n_total + 1
  }
  if (!isTRUE(m$exceededTransferLimit)) break
  off <- off + 2000
}
cat(sprintf("Done. tiles listed: %d  saved to ./gfw_tiles/\n", n_total))

```


#02_cleanのメモ
# --- 既存 ---
# setnames(dt, intersect(names(dt), c("hotspot_n","hotspot","count")), "hotspot_n", skip_absent = TRUE)

# --- 置き換え ---
safe_rename(dt, c("hotspot_n","hotspot","count","n_hotspot","hotspots"), "hotspot_n")

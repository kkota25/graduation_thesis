# R/utils_io.R
fast_read_csv <- function(path, select = NULL, colClasses = NULL) {
  data.table::setDTthreads(percent = 90)
  dt <- data.table::fread(
    input = path,
    select = select,
    colClasses = colClasses,
    showProgress = interactive(),
    nThread = data.table::getDTthreads()
  )
  return(dt)
}

write_parquet_safely <- function(df, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  arrow::write_parquet(df, path, compression = "zstd")
}

read_parquet_ds <- function(path_glob_or_dir) {
  ds <- arrow::open_dataset(path_glob_or_dir)
  return(ds)
}

# ファイル名から region / period を抜く正規表現ヘルパ
parse_region_period <- function(fname) {
  region <- sub(".*adm2_([^_]+)_.*", "\\1", fname)
  region <- gsub("_", " ", region)
  period <- sub(".*_(2019_2021|2022_2025).*", "\\1", fname)
  data.frame(region = region, period = period, stringsAsFactors = FALSE)
}

# ---- utils_io.R ----
years_in_period <- function(p){
  if (length(p) != 1L) stop("years_in_period: length must be 1, got ", length(p))
  s <- as.character(p)
  if (is.na(s) || !nzchar(s)) stop("years_in_period: period is NA/blank")
  
  # 代表的ケースを早取り
  if (s %in% c("2019_2021","2022_2025")) {
    yy <- strsplit(s, "_", fixed = TRUE)[[1]]
    return(seq.int(as.integer(yy[1]), as.integer(yy[2])))
  }
  # 文字列中から "YYYY_YYYY" を抽出して年列を返す
  m <- regexpr("(19|20)\\d{2}_(19|20)\\d{2}", s, perl = TRUE)
  if (m[1] > 0) {
    r  <- regmatches(s, m)
    yy <- strsplit(r, "_", fixed = TRUE)[[1]]
    a  <- as.integer(yy[1]); b <- as.integer(yy[2])
    if (is.na(a) || is.na(b) || a > b) stop("years_in_period: invalid range in '", s, "'")
    return(seq.int(a, b))
  }
  stop("years_in_period: Unknown period: ", s)
}


# ---------- 期間→年 展開 ----------
expand_period <- function(dt, value_cols, how = c("copy","split")){
  how <- match.arg(how)
  stopifnot(all(c("adm2_code","period") %in% names(dt)))
  
  # 数値化（できない値は NA）
  dt2 <- copy(dt)
  for (nm in intersect(value_cols, names(dt2))) {
    dt2[[nm]] <- suppressWarnings(as.numeric(dt2[[nm]]))
  }
  
  # 出力入れ物
  out_list <- vector("list", length = length(unique(dt2$period)))
  i <- 0L
  
  for (per in unique(dt2$period)) {
    if (is.na(per) || !nzchar(per)) next
    yrs <- years_in_period(as.character(per))
    L   <- length(yrs)
    
    # periodごとのサブセット × 年テーブルを直積（行を年数分に複製）
    sub <- dt2[period == per]
    if (nrow(sub) == 0) next
    years_dt <- data.table(period = per, year = yrs)
    sub_exp  <- merge(sub, years_dt, by = "period", allow.cartesian = TRUE)
    
    # 値列を copy/split
    if (length(value_cols)) {
      if (how == "copy") {
        # そのまま（各年に同じ値）
        # 何もしない
      } else {
        # 年数で割る
        sub_exp[, (value_cols) := lapply(.SD, function(x) x / L), .SDcols = value_cols]
      }
    }
    
    i <- i + 1L
    out_list[[i]] <- sub_exp
  }
  
  out <- rbindlist(out_list[seq_len(i)], use.names = TRUE, fill = TRUE)
  # 余分な列を整理（順序はお好みで）
  keep <- unique(c("adm1_name","adm2_code","adm2_name","region","year","period", value_cols))
  keep <- intersect(keep, names(out))
  out[, ..keep][]
}
# scripts/05_descriptives.R
# ------------------------------------------------------------
# 目的: data/processed/adm2_panel.parquet から
#      「アラート関連を除く数値列」の記述統計を作成し保存
# 出力:
#   - outputs/tables/descriptives_overall.csv
#   - outputs/tables/descriptives_by_year.csv
#   - reports/outputs/tables/descriptives_overall.tex
#   - reports/outputs/tables/descriptives_by_year.tex
# 依存: data.table, arrow, here, xtable
# ------------------------------------------------------------

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
  library(here)
  library(xtable)
})

dir.create(here("outputs","tables"), recursive = TRUE, showWarnings = FALSE)
dir.create(here("reports","outputs","tables"), recursive = TRUE, showWarnings = FALSE)

# ---- 1変数の統計量 ----
.one_var_stats <- function(x, total_n){
  n  <- sum(!is.na(x))
  nz <- if (n > 0) sum(x == 0, na.rm = TRUE) else NA_integer_
  data.frame(
    n          = n,
    mean       = if (n > 0) mean(x, na.rm = TRUE) else NA_real_,
    sd         = if (n > 1)  sd(x,   na.rm = TRUE) else NA_real_,
    min        = if (n > 0) min(x,   na.rm = TRUE) else NA_real_,
    p25        = if (n > 0) as.numeric(quantile(x, 0.25, na.rm = TRUE, type = 2)) else NA_real_,
    median     = if (n > 0) as.numeric(quantile(x, 0.50, na.rm = TRUE, type = 2)) else NA_real_,
    p75        = if (n > 0) as.numeric(quantile(x, 0.75, na.rm = TRUE, type = 2)) else NA_real_,
    max        = if (n > 0) max(x,   na.rm = TRUE) else NA_real_,
    zero_share = if (!is.na(nz) && n > 0) nz / n else NA_real_,
    miss       = total_n - n,
    miss_share = if (total_n > 0) (total_n - n) / total_n else NA_real_,
    check.names = FALSE
  )
}

# ---- 複数列の統計 ----
.multi_stats <- function(DT, vars){
  total_n <- nrow(DT)
  out <- lapply(vars, function(v){
    x <- DT[[v]]
    if (!is.numeric(x)) x <- suppressWarnings(as.numeric(x))
    st <- .one_var_stats(x, total_n)
    cbind(variable = v, st, row.names = NULL)
  })
  data.table::rbindlist(out, use.names = TRUE, fill = TRUE)
}

# ---- xtable 用 digits ベクトル作成 ----
.make_digits <- function(df, int_cols = c("n","miss")){
  c(0, sapply(names(df), function(nm){
    if (nm %in% c("variable")) 0
    else if (nm %in% int_cols) 0
    else 3
  }))
}

# ---- メイン ----
run_descriptives <- function(panel_path){
  if (!file.exists(panel_path)) stop("パネルファイルが見つかりません: ", panel_path)
  DT <- as.data.table(arrow::read_parquet(panel_path))
  
  # 除外規則
  id_like    <- grepl("^adm\\d|_code$|_name$|^year$|^geometry$", names(DT), ignore.case = TRUE)
  alert_like <- grepl("alert", names(DT), ignore.case = TRUE)
  is_num     <- vapply(DT, function(x) is.numeric(x) || is.integer(x), logical(1))
  target_vars <- names(DT)[is_num & !id_like & !alert_like]
  if (length(target_vars) == 0L) stop("対象となる数値列（アラート以外）が見つかりません。")
  
  # 全期間
  tab_all <- .multi_stats(DT, target_vars)[
    , .(variable, n, mean, sd, min, p25, median, p75, max, zero_share, miss, miss_share)
  ]
  
  # 年別
  if (!"year" %in% names(DT)) stop("year 列がありません。年別統計は作れません。")
  tab_by_year <- DT[, .multi_stats(.SD, target_vars), by = year][
    , .(year, variable, n, mean, sd, min, p25, median, p75, max, zero_share, miss, miss_share)
  ][order(variable, year)]
  
  # CSV
  fwrite(tab_all,     here("outputs","tables","descriptives_overall.csv"))
  fwrite(tab_by_year, here("outputs","tables","descriptives_by_year.csv"))
  
  # TeX（xfun 非依存: xtable で書き出し）
  dig_all <- .make_digits(tab_all)
  dig_by  <- .make_digits(tab_by_year, int_cols = c("n","miss","year"))
  
  xt_all <- xtable(tab_all,
                   caption = "記述統計（アラート以外, 全期間）",
                   label   = "tab:desc_overall",
                   align   = NULL,
                   digits  = dig_all)
  xt_by  <- xtable(tab_by_year,
                   caption = "記述統計（アラート以外, 年別）",
                   label   = "tab:desc_byyear",
                   align   = NULL,
                   digits  = dig_by)
  
  print(xt_all,
        include.rownames = FALSE,
        booktabs = TRUE,
        sanitize.text.function = identity,
        na.string = "",
        file = here("reports","outputs","tables","descriptives_overall.tex"))
  
  print(xt_by,
        include.rownames = FALSE,
        booktabs = TRUE,
        sanitize.text.function = identity,
        na.string = "",
        file = here("reports","outputs","tables","descriptives_by_year.tex"))
  
  invisible(list(
    overall   = tab_all,
    by_year   = tab_by_year,
    vars_used = target_vars
  ))
}

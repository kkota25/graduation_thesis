# scripts/04_baseline_analysis.R
# ---------------------------------------------
# 目的：加工済みパネルを読み込み、QC → ベースライン回帰 → ロバスト性 → 図表/表の保存
# 依存：data/processed/adm2_panel.parquet（03_build_processed.R で作成済）
# 出力：outputs/tables/*.tex, outputs/figures/*.png
# ---------------------------------------------

# ---- 0) パッケージ・準備 ----
req <- c("data.table","arrow","fixest","ggplot2","dplyr")
ins <- req[!sapply(req, requireNamespace, quietly=TRUE)]
if (length(ins)) install.packages(ins)
library(data.table); library(arrow); library(fixest); library(ggplot2); library(dplyr)

dir.create("outputs/tables",  recursive = TRUE, showWarnings = FALSE)
dir.create("outputs/figures", recursive = TRUE, showWarnings = FALSE)

# ---- FE後分散 & 特異値チェック ----
library(data.table)

.fe_demean <- function(dt, vars, fe_i="adm2_code", fe_t="year"){
  # 2方向FEのdemeanを手計算
  Z <- copy(dt)
  for(v in vars){
    mu_i <- ave(Z[[v]], Z[[fe_i]], FUN=mean)
    mu_t <- ave(Z[[v]], Z[[fe_t]], FUN=mean)
    mu   <- mean(Z[[v]], na.rm=TRUE)
    Z[[v]] <- Z[[v]] - mu_i - mu_t + mu
  }
  as.data.frame(Z[, ..vars])
}

check_fe_singularity <- function(dt, y, xvars, fe_i="adm2_code", fe_t="year"){
  req <- c(y, xvars, fe_i, fe_t)
  s <- dt[complete.cases(dt[, ..req])]
  if(nrow(s) < 50L){
    return(list(ok=FALSE, msg=sprintf("rows after complete.cases too small: n=%d", nrow(s))))
  }
  # FE後のy分散
  y_dm <- .fe_demean(s, y, fe_i, fe_t)[[1]]
  v_within <- var(y_dm)
  if(!is.finite(v_within) || v_within==0) {
    return(list(ok=FALSE, msg="dependent var has zero within-variance under | adm2_code + year"))
  }
  # Xの特異値（QR階数）
  X_dm <- as.matrix(.fe_demean(s, xvars, fe_i, fe_t))
  qx <- qr(X_dm)
  r  <- qx$rank; k <- ncol(X_dm)
  dep_idx <- setdiff(seq_len(k), qx$pivot[seq_len(r)])
  dep_names <- if(length(dep_idx)) colnames(X_dm)[dep_idx] else character(0)
  list(ok=(r==k), msg=if(r<k) sprintf("rank %d < %d; dependent: %s", r, k, paste(dep_names, collapse=", ")) else "full rank",
       n=nrow(s), rank=r, k=k, dep=dep_names, v_within=v_within)
}




# ---- 1) ロード ----
load_panel <- function(path = "data/processed/adm2_panel.parquet") {
  p <- as.data.table(arrow::read_parquet(path))
  # 型と基本クレンジング（念のため）
  p[, `:=`(
    adm2_code = as.character(adm2_code),
    year      = as.integer(year)
  )]
  setorder(p, adm2_code, year)
  p
}

# ---- 2) QC（軽く状況を把握） ----
qc_summary <- function(p) {
  msg <- capture.output({
    cat("\n[QC] 行数・列数: ", nrow(p), " x ", ncol(p), "\n")
    cat("[QC] 年の範囲: ", paste(range(p$year, na.rm=TRUE), collapse=" - "), "\n")
    cat("[QC] 欠損率(%)：\n")
    print(p[, lapply(.SD, function(x) mean(is.na(x))*100),
            .SDcols=c("cloud_share","clear_share","alerts","alerts_adj",
                      "loss_ha","burned_ha","hotspot_n","chirps_mm","viirs_ntl")])
  })
  writeLines(msg)
  invisible(p)
}

# ---- 3) ベースライン推定 ----

fit_iv <- function(p) {
  # --- 追加: 年別の一意数を確認して保存し、定数なら明示停止 ---
  dir.create("outputs/debug", recursive = TRUE, showWarnings = FALSE)
  diag_tab <- as.data.table(p)[, .(
    alerts_uniq      = data.table::uniqueN(alerts,      na.rm = TRUE),
    alerts_adj_uniq  = data.table::uniqueN(alerts_adj,  na.rm = TRUE),
    cloud_share_uniq = data.table::uniqueN(cloud_share, na.rm = TRUE)
  ), by = year][order(year)]
  data.table::fwrite(diag_tab, "reports/outputs/debug/alerts_uniqs_by_year.csv")
  
  if (data.table::uniqueN(p$alerts, na.rm=TRUE)     <= 1L &&
      data.table::uniqueN(p$alerts_adj, na.rm=TRUE) <= 1L) {
    stop("alerts/alerts_adj が定数です。outputs/debug/alerts_uniqs_by_year.csv を確認してください。")
  }


  
  # --- 第一段階（ln_alerts_raw を従属） ---
  fs <- feols(
    ln_alerts_raw ~ cloud_share + chirps_mm + viirs_ntl + ln_burn + ln_hotspot |
      adm2_code + year,
    cluster = ~ adm2_code, data = p_use
  )
  
  # --- 第二段階（ln_alerts_raw を内生、cloud_share を計器） ---
  iv2 <- feols(
    ln_loss ~ chirps_mm + viirs_ntl + ln_burn + ln_hotspot | adm2_code + year,
    iv = ~ ln_alerts_raw ~ cloud_share,
    cluster = ~ adm2_code, data = p_use
  )
  
  list(first_stage = fs, second_stage = iv2, panel = p_use)
}







# ---- 4) ロバスト性（例） ----
# (a) 年範囲：2019-2024 に限定（forestloss欠測対策）
# (b) cloud_share を説明から外す（alerts_adj は既に clear_share で割っているため）
# (c) 雨季降水（あれば）を使う仕様
fit_robust <- function(p) {
  # 2019-2024に限定
  pd24 <- subset(as.data.frame(p), year >= 2019 & year <= 2024)
  
  r_iv_noS2 <- feols(
    ln_loss ~ chirps_mm + viirs_ntl + ln_burn + ln_hotspot | adm2_code + year,
    iv = ~ ln_alerts_adj ~ cloud_share,
    cluster = ~ adm2_code, data = pd24
  )
  
  r_iv_rainy <- NULL
  if ("chirps_rainy_mm" %in% names(pd24)) {
    r_iv_rainy <- feols(
      ln_loss ~ chirps_rainy_mm + viirs_ntl + ln_burn + ln_hotspot | adm2_code + year,
      iv = ~ ln_alerts_adj ~ cloud_share,
      cluster = ~ adm2_code, data = pd24
    )
  }
  
  list(r_iv_noS2 = r_iv_noS2, r_iv_rainy = r_iv_rainy)
}


# ---- 5) 図表の保存 ----
# 依存
library(ggplot2)

save_tables <- function(iv_fit, robust, outdir = here::here("outputs/tables")) {
  dir.create(outdir, recursive = TRUE, showWarnings = FALSE)
  
  # 第一段階と第二段階を並べる
  base_list <- Filter(Negate(is.null), list(iv_fit$first_stage, iv_fit$second_stage, iv_fit$second_stage_raw))
  if (length(base_list)) {
    fixest::etable(
      base_list,
      tex = TRUE,
      file = file.path(outdir, "baseline.tex"),
      title = "First stage and 2SLS (ADM2 & Year FE, cluster=ADM2)",
      replace = TRUE, digits = 3,
      fitstat = ~ n + r2 + ar2 + ivf + weakiv + partial_f + partial_r2
    )
  }
  
  rob_list <- Filter(Negate(is.null), robust)
  if (length(rob_list)) {
    do.call(
      fixest::etable,
      c(rob_list, list(
        tex = TRUE,
        file = file.path(outdir,"robustness.tex"),
        title = "Robustness (2SLS variants)",
        replace = TRUE, digits = 3,
        fitstat = ~ n + r2 + ar2 + ivf + weakiv + partial_f + partial_r2
      ))
    )
  }
  invisible(NULL)
}

# 4) 図（列名統一）
save_figures <- function(p, outdir = here::here("outputs/figures")) {
  dir.create(outdir, recursive = TRUE, showWarnings = FALSE)
  df <- as.data.frame(p)
  
  g1 <- ggplot(df, aes(ln_alerts_adj)) +
    geom_histogram(bins=40) + facet_wrap(~year) +
    labs(x="ln_alerts_adj = log1p(alerts_adj)", y="Count", title="Visibility-adjusted alerts")
  ggsave(file.path(outdir,"hist_ln_alerts_adj.png"), g1, width=9, height=5, dpi=150)
  
  g2 <- ggplot(df, aes(cloud_share, ln_alerts_adj)) +
    geom_point(alpha=.3) + geom_smooth(method="lm") +
    labs(x="Cloud share (S2)", y="ln_alerts_adj = log1p(alerts_adj)", title="Clouds vs alerts (first-stage sign check)")
  ggsave(file.path(outdir,"scatter_clouds_alerts.png"), g2, width=6, height=4, dpi=150)
  
  invisible(NULL)
}

run_all <- function(parquet_path = "data/processed/adm2_panel.parquet"){
  panel <- data.table::as.data.table(arrow::read_parquet(parquet_path))
  
  eps <- 1e-6
  panel[, clear_share_fix := fifelse(is.finite(clear_share) & clear_share > eps, clear_share, eps)]
  panel[, alerts_adj := alerts / clear_share_fix]
  panel[, ln_alerts_adj := log1p(alerts_adj)]
  
  ivfit <- fit_iv(panel)
  rob   <- fit_robust(panel)
  save_tables(ivfit, rob)
  save_figures(panel)
  list(panel=panel, iv=ivfit, robust=rob)
}

run_baseline <- function(p){
  vars <- c("ln_alerts_adj","cloud_share","chirps_mm","viirs_ntl","ln_burn")
  s <- p[complete.cases(p[, ..vars])]
  m1 <- fixest::feols(
    ln_alerts_adj ~ cloud_share + chirps_mm + viirs_ntl + ln_burn |
      adm2_code + year,
    data = s
  )
  list(model = m1)
}

# 実行例：
# res <- run_all()
# fixest::etable(res$baseline$m1)

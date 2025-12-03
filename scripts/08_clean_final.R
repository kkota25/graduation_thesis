# scripts/08_clean_final.R

library(tidyverse)
library(arrow)

# 前処理ユーティリティ（write_parquet_safely など）
source("R/utils_io.R")

# --------------------------------------------------
# 1. 入力データの読み込み
# --------------------------------------------------
panel_path <- "data/processed/adm2_panel.parquet"

if (!file.exists(panel_path)) {
  stop("adm2_panel.parquet が存在しません。先に 07_build_processed.R を実行してください。")
}

panel <- read_parquet(panel_path)

# --------------------------------------------------
# 2. サンプル制限・変数生成
# --------------------------------------------------
reg_dt <- panel %>%
  # 期間：2019–2024 年のみ
  filter(year >= 2019, year <= 2024) %>%
  # 森林がほとんどない ADM2 を除外（閾値は調整可）
  filter(forest2000_ha > 1000) %>%
  # 使用変数を整理（hotspot, ntl などは使わない）
  select(
    adm1_name, adm2_name, adm2_code, year,
    ha_alerts,
    chirps_mm, chirps_rainy_mm,
    cloud_share,
    burned_ha,
    defor_rate,
    forest2000_ha,
    loss_ha
  ) %>%
  # ログ変数を作成
  mutate(
    ln_alerts = log1p(ha_alerts),   # = log(ha_alerts + 1)
    ln_loss   = log1p(loss_ha),     # = log(loss_ha   + 1)
    ln_burned = log1p(burned_ha)    # = log(burned_ha + 1)
  ) %>%
  # 主要変数に欠損がある行を落とす
  drop_na(
    ha_alerts, ln_alerts,
    loss_ha, ln_loss,
    defor_rate,
    cloud_share,
    chirps_mm,
    burned_ha, ln_burned
  )

# --------------------------------------------------
# 3. 分析用パネルの書き出し
# --------------------------------------------------
out_path <- "data/processed/adm2_reg_2019_2024.parquet"
write_parquet_safely(reg_dt, out_path)

# --------------------------------------------------
# 4. チェック用出力
# --------------------------------------------------
cat("\nSaved analysis dataset to:", out_path, "\n")
cat("Number of rows:", nrow(reg_dt), "\n")
cat("Number of columns:", ncol(reg_dt), "\n\n")

print(summary(reg_dt$ha_alerts))

# scripts/06_histograms.R
# ------------------------------------------------------------
# 目的: 「アラート以外の数値列」ごとにヒストグラムを作成し保存
# 入力: data/processed/adm2_panel.parquet
# 出力: 
#   - outputs/figures/hist/<variable>_hist.png
#   - reports/outputs/figures/histograms_all.pdf（全変数まとめ）
# 依存: data.table, arrow, here, ggplot2, fs
# ------------------------------------------------------------
# パッケージ読み込み
library(arrow)
library(ggplot2)
library(dplyr)

# データ読み込み
reg_dt <- read_parquet("data/processed/adm2_reg_2019_2024.parquet")

# ha_alerts
ggplot(reg_dt, aes(x = ha_alerts)) +
  geom_histogram(bins = 30) +
  labs(
    title = "Histogram of ha_alerts",
    x = "ha_alerts",
    y = "Count"
  )

# chirps_mm
ggplot(reg_dt, aes(x = chirps_mm)) +
  geom_histogram(bins = 30) +
  labs(
    title = "Histogram of chirps_mm",
    x = "chirps_mm",
    y = "Count"
  )

# cloud_share
ggplot(reg_dt, aes(x = cloud_share)) +
  geom_histogram(bins = 30) +
  labs(
    title = "Histogram of cloud_share",
    x = "cloud_share",
    y = "Count"
  )

# burned_ha
ggplot(reg_dt, aes(x = burned_ha)) +
  geom_histogram(bins = 30) +
  labs(
    title = "Histogram of burned_ha",
    x = "burned_ha",
    y = "Count"
  )

# defor_rate
ggplot(reg_dt, aes(x = defor_rate)) +
  geom_histogram(bins = 30) +
  labs(
    title = "Histogram of defor_rate",
    x = "defor_rate",
    y = "Count"
  )

## ln_alerts のヒストグラム
ggplot(reg_dt, aes(x = ln_alerts)) +
  geom_histogram(bins = 30) +
  labs(
    title = "Histogram of ln_alerts",
    x = "ln(ha_alerts + 1)",
    y = "Count"
  )

## ln_loss のヒストグラム
ggplot(reg_dt, aes(x = ln_loss)) +
  geom_histogram(bins = 30) +
  labs(
    title = "Histogram of ln_loss",
    x = "ln(loss_ha + 1)",
    y = "Count"
  )


ggplot(reg_dt, aes(x = ln_burned)) +
  geom_histogram(bins = 30) +
  labs(
    title = "Histogram of ln_burned",
    x = "ln(burned_ha + 1)",
    y = "Count"
  )


#分布を見るために0付近の値を取り除く

# ln_alerts（ha_alerts > 0 のみ）
ggplot(reg_dt %>% filter(ha_alerts > 0),
       aes(x = ln_alerts)) +
  geom_histogram(bins = 30) +
  labs(
    title = "Histogram of ln_alerts (ha_alerts > 0)",
    x = "ln(ha_alerts + 1)",
    y = "Count"
  )

# ln_burned（burned_ha > 0 のみ）
ggplot(reg_dt %>% filter(burned_ha > 0),
       aes(x = ln_burned)) +
  geom_histogram(bins = 30) +
  labs(
    title = "Histogram of ln_burned (burned_ha > 0)",
    x = "ln(burned_ha + 1)",
    y = "Count"
  )

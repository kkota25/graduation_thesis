# scripts/11_scatterplots.R
library(arrow)
library(dplyr)
library(ggplot2)

reg_dt <- read_parquet("data/processed/adm2_reg_2019_2024.parquet")

# 見やすさのため，従属変数側だけ上位1%をカットしたサンプルを使う関数
trim_top1 <- function(df, var) {
  up <- quantile(df[[var]], 0.99, na.rm = TRUE)
  df %>% filter(.data[[var]] <= up)
}

## ① ln_alerts vs defor_rate
df1 <- trim_top1(reg_dt, "defor_rate")

ggplot(df1, aes(x = ln_alerts, y = defor_rate)) +
  geom_point(alpha = 0.3) +
  geom_smooth(method = "lm", se = FALSE) +
  labs(
    title = "Deforestation rate vs ln_alerts",
    x = "ln(ha_alerts + 1)",
    y = "Deforestation rate"
  )

## ② ln_alerts vs ln_loss
df2 <- trim_top1(reg_dt, "ln_loss")

ggplot(df2, aes(x = ln_alerts, y = ln_loss)) +
  geom_point(alpha = 0.3) +
  geom_smooth(method = "lm", se = FALSE) +
  labs(
    title = "ln_loss vs ln_alerts",
    x = "ln(ha_alerts + 1)",
    y = "ln(loss_ha + 1)"
  )

## ③ cloud_share vs ln_alerts（第1段階）
ggplot(reg_dt, aes(x = cloud_share, y = ln_alerts)) +
  geom_point(alpha = 0.3) +
  geom_smooth(method = "lm", se = FALSE) +
  labs(
    title = "ln_alerts vs cloud_share (first stage)",
    x = "Cloud share",
    y = "ln(ha_alerts + 1)"
  )

## ④ cloud_share vs defor_rate（reduced form 1）
df4 <- trim_top1(reg_dt, "defor_rate")

ggplot(df4, aes(x = cloud_share, y = defor_rate)) +
  geom_point(alpha = 0.3) +
  geom_smooth(method = "lm", se = FALSE) +
  labs(
    title = "Deforestation rate vs cloud_share (reduced form)",
    x = "Cloud share",
    y = "Deforestation rate"
  )

## ⑤ cloud_share vs ln_loss（reduced form 2）
df5 <- trim_top1(reg_dt, "ln_loss")

ggplot(df5, aes(x = cloud_share, y = ln_loss)) +
  geom_point(alpha = 0.3) +
  geom_smooth(method = "lm", se = FALSE) +
  labs(
    title = "ln_loss vs cloud_share (reduced form)",
    x = "Cloud share",
    y = "ln(loss_ha + 1)"
  )

## ⑥ cloud_share vs chirps_mm（IV とコントロールの関係）
ggplot(reg_dt, aes(x = cloud_share, y = chirps_mm)) +
  geom_point(alpha = 0.3) +
  geom_smooth(method = "lm", se = FALSE) +
  labs(
    title = "chirps_mm vs cloud_share",
    x = "Cloud share",
    y = "CHIRPS rainfall (mm)"
  )



#1. **ln_alerts vs defor_rate**
  
#  * 目的：処置変数（アラート）と主要アウトカム（森林減少率）の生の関係を可視化し、アラート増加と減少率の関係の方向・強さを直感的に確認する。

#2. **ln_alerts vs ln_loss**
  
#  * 目的：もう一つのアウトカム（森林減少面積の対数）との関係を確認し、処置の影響が別定義のアウトカムでも一貫していそうかを見る。

#3. **cloud_share vs ln_alerts（第1段階）**
  
#  * 目的：IV（cloud_share）が処置変数 ln_alerts をどの程度強く説明しているか（第1段階の強さと符号）を視覚的に確認する。

#4. **cloud_share vs defor_rate（reduced form 1）**
  
#  * 目的：IV がアウトカム defor_rate に与える総合的な効果（reduced form）の符号・形を確認し、2SLS の推定結果と整合的かを見る。

#5. **cloud_share vs ln_loss（reduced form 2）**
  
#  * 目的：別アウトカム ln_loss に対する reduced form を確認し、IV→アウトカムの関係が定義を変えても似た方向性かをチェックする。

#6. **cloud_share vs chirps_mm（IV とコントロール）**
  
#  * 目的：IV が主要コントロール（降水量）とどの程度相関しているかを確認し、「IV が観測可能な環境要因と強く結びつきすぎていないか」（外生性の直感チェック）を見る。

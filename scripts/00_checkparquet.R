# RStudio の Console で（プロジェクトのルートから）
library(arrow)
library(dplyr)
library(here)



########全体確認#########

names(df_alert)
names(df_chirps)
names(df_burned)
names(df_cloud)
names(df_forestloss)
names(df_viirs)
names(hotspot)

##################### adm2_reg_2019_2024.parquet #####################
df <- arrow::read_parquet(
  here("data", "processed", "adm2_reg_2019_2024.parquet")
)

# 構造だけサッと見る
dplyr::glimpse(df)

# 先頭だけ見る
head(df, 5)

# RStudio で表形式で見たいとき
View(df)

summary(df)


##################### adm2_panel.parquet #####################
df_panel <- arrow::read_parquet(
  here("data", "processed", "adm2_panel.parquet")
)

# 構造だけサッと見る
dplyr::glimpse(df_panel)

# 先頭だけ見る
head(df_panel, 20)

# RStudio で表形式で見たいとき
View(df_panel)

summary(df_panel)


#################### alerts_year.parquet #####################

df_alert <- arrow::read_parquet(
  here("data", "interim", "alerts_year.parquet")
)

# 構造だけサッと見る
dplyr::glimpse(df_alert)

# 先頭だけ見る
head(df_alert, 20)

# RStudio で表形式で見たいとき
View(df_alert)

#####################chirps#####################
df_chirps <- arrow::read_parquet(
  here("data", "interim", "chirps_year.parquet")
)

# 構造だけサッと見る
dplyr::glimpse(df_chirps)

# 先頭だけ見る
head(df_chirps, 20)

# RStudio で表形式で見たいとき
View(df_chirps)


##################### burned_period.parquet #####################
df_burned <- arrow::read_parquet(
  here("data", "interim", "burned_period.parquet")
)

# 構造だけサッと見る
dplyr::glimpse(df_burned)

# 先頭だけ見る
head(df_burned, 20)

# RStudio で表形式で見たいとき
View(df_burned)

##################### clouds_period.parquet #####################
df_cloud <- arrow::read_parquet(
  here("data", "interim", "clouds_period.parquet")
)

# 構造だけサッと見る
dplyr::glimpse(df_cloud)

# 先頭だけ見る
head(df_cloud, 20)

# RStudio で表形式で見たいとき
View(df_cloud)


##################### forestloss_period.parquet #####################
df_forestloss <- arrow::read_parquet(
  here("data", "interim", "forestloss_year.parquet")
)

# 構造だけサッと見る
dplyr::glimpse(df_forestloss)

# 先頭だけ見る
head(df_forestloss, 20)

# RStudio で表形式で見たいとき
View(df_forestloss)


##################### viirs_year.parquet #####################
df_viirs <- arrow::read_parquet(
  here("data", "interim", "viirs_year.parquet")
)

# 構造だけサッと見る
dplyr::glimpse(df_viirs)

# 先頭だけ見る
head(df_viirs, 20)

# RStudio で表形式で見たいとき
View(df_viirs)




##################### hotspot_period.parquet #####################

# 全て欠損値で問題あり！！！！！！！！！

df_hotspot <- arrow::read_parquet(
  here("data", "interim", "hotspot_period.parquet")
)

# 構造だけサッと見る
dplyr::glimpse(df_hotspot)

# 先頭だけ見る
head(df_hotspot, 20)

# RStudio で表形式で見たいとき
View(df_hotspot)

# 1. どんな値があるか一覧（小さい順）
sort(unique(df_hotspot$hotspot_n))

# 3. 数値としての概要（最小・最大など）
summary(df_hotspot$hotspot_n)

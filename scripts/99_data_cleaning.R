# 99_data_cleaning.R

## ステップ1 #############################################

# Burgess (2012) に倣って main forest islands のみ残す
#   - 入力: reg_dyn（summary(reg_dyn) のデータフレーム）
#   - 出力:
#       * drop_adm1: 落とす州
#       * keep_adm1: 残す州
#       * reg_dyn_main_islands: main forest islands のみのデータ


# 1. 落とすべき adm1_name（Java, Bali, Nusa Tenggara, Maluku, Riau Islands）
drop_adm1 <- c(
  # Java 地方
  "Banten",
  "Dki Jakarta",
  "Jawa Barat",
  "Jawa Tengah",
  "Jawa Timur",
  "Daerah Istimewa Yogyakarta",
  
  # Bali + Nusa Tenggara（Lesser Sunda）
  "Bali",
  "Nusatenggara Barat",
  "Nusatenggara Timur",
  
  # Maluku
  "Maluku",
  "Maluku Utara",
  
  # Riau Islands（本土 Riau とは別）
  "Kepulauan-riau"
)

# 2. 実際に残る adm1_name を確認
keep_adm1 <- sort(setdiff(unique(reg_dyn$adm1_name), drop_adm1))

keep_adm1
# 想定される出力:
# [1] "Bangka Belitung"           
# [2] "Bengkulu"                  
# [3] "Gorontalo"                 
# [4] "Jambi"                     
# [5] "Kalimantan Barat"          
# [6] "Kalimantan Selatan"        
# [7] "Kalimantan Tengah"         
# [8] "Kalimantan Timur"          
# [9] "Lampung"                   
# [10] "Nangroe Aceh Darussalam"   
# [11] "Papua"                     
# [12] "Papua Barat"               
# [13] "Riau"                      
# [14] "Sulawesi Barat"            
# [15] "Sulawesi Selatan"          
# [16] "Sulawesi Tengah"           
# [17] "Sulawesi Tenggara"         
# [18] "Sulawesi Utara"            
# [19] "Sumatera Barat"            
# [20] "Sumatera Selatan"          
# [21] "Sumatera Utara"            

# ※ Bangka Belitung は Sumatra 本土ではない島嶼州なので、
#   落としたい場合は drop_adm1 に "Bangka Belitung" を追加して、
#   上の 2 行をもう一度実行してください。

# 3. main forest islands のみを残したデータを作成
reg_dyn_main_islands <- reg_dyn %>%
  filter(!adm1_name %in% drop_adm1)

# チェック用：州ごとの観測数を確認
reg_dyn_main_islands %>%
  count(adm1_name, sort = TRUE)

## ステップ2 #############################################
# 1. 2000年森林率（forest2000_ha / total_ha）を作成 ----------------
reg_dyn_main_islands <- reg_dyn_main_islands %>%
  mutate(
    share_forest2000 = forest2000_ha / total_ha
  )

# summary(reg_dyn_main_islands$share_forest2000)

# 2. 閾値 2%（0.02）未満の ADM2 を落とす ----------------------------
#   - 「森林率が 2% 以上」の ADM2 だけ残す
reg_dyn_forest02 <- reg_dyn_main_islands %>%
  filter(share_forest2000 >= 0.02)

# チェック：州ごとの観測数
#reg_dyn_forest02 %>%
#  count(adm1_name, sort = TRUE)

# 2. 閾値 5%（0.05）未満の ADM2 を落とす ----------------------------
#   - 「森林率が 2% 以上」の ADM2 だけ残す
# reg_dyn_forest05 <- reg_dyn_main_islands %>%
#   filter(share_forest2000 >= 0.05)

# チェック：州ごとの観測数
#reg_dyn_forest05 %>%
#  count(adm1_name, sort = TRUE)


## ステップ3 #############################################

# Step 3: 2019–2024 の loss_ha / defor_rate が「ほぼゼロ」の ADM2 を除外
#   - 入力: reg_dyn_forest02
#   - 出力:
#       * dyn_frontier_stats : ADM2 ごとの要約
#       * reg_dyn_frontier_loose : 緩めのフロンティア定義
#       * reg_dyn_frontier_strict: 厳しめのフロンティア定義（ロバストネス）



# 0. 念のため、対象期間を明示（reg_dyn は既に 2020–2024 のはずだが）
reg_dyn_forest02 <- reg_dyn_forest02 %>%
  filter(year >= 2019, year <= 2024)

# 1. ADM2 ごとに deforestation の要約統計を計算 --------------------
dyn_frontier_stats <- reg_dyn_forest02 %>%
  group_by(adm2_code, adm1_name, adm2_name) %>%
  summarise(
    n_years        = n(),
    mean_loss_ha   = mean(loss_ha, na.rm = TRUE),
    max_loss_ha    = max(loss_ha,  na.rm = TRUE),
    mean_defor     = mean(defor_rate, na.rm = TRUE),
    max_defor      = max(defor_rate,  na.rm = TRUE),
    .groups = "drop"
  )

# 分布の確認（必要なら）
summary(dyn_frontier_stats$mean_loss_ha)
summary(dyn_frontier_stats$max_loss_ha)
summary(dyn_frontier_stats$mean_defor)
summary(dyn_frontier_stats$max_defor)

# 1. 四分位点から閾値を計算

q_max_loss  <- quantile(dyn_frontier_stats$max_loss_ha,  probs = c(0.25, 0.5), na.rm = TRUE)
q_max_defor <- quantile(dyn_frontier_stats$max_defor,    probs = c(0.25, 0.5), na.rm = TRUE)

q_max_loss
q_max_defor


# q_max_loss[1] は max_loss_ha の25パーセンタイル（下位25%と上位75%の境界）
# q_max_loss[2] は max_loss_ha の中央値（50%点）
# q_max_defor も同様に max_defor の25%点と中央値

# 2. フロンティア指標を定義
#    - loose: 1四分位以上の 「最大損失面積が大きい」または「最大損失率が大きい」 を経験した ADM2
#    - strict: 中央値以上の 「最大損失面積が大きい」または「最大損失率が大きい」 を経験した ADM2

dyn_frontier_stats <- dyn_frontier_stats %>%
  mutate(
    # 緩めの定義（main spec）
    is_frontier_loose = (max_loss_ha >= q_max_loss[1]) |
      (max_defor   >= q_max_defor[1]),
    
    # 厳しめの定義（robustness）
    is_frontier_strict = (max_loss_ha >= q_max_loss[2]) |
      (max_defor   >= q_max_defor[2])
  )

# どれくらい残るか確認
dyn_frontier_stats %>%
  summarise(
    n_total  = n(),
    n_loose  = sum(is_frontier_loose),
    n_strict = sum(is_frontier_strict)
  )


# 3. ADM2 フラグを元データ（reg_dyn_forest02）に戻す

reg_dyn_frontier <- reg_dyn_forest02 %>%
  left_join(
    dyn_frontier_stats %>%
      select(adm2_code, is_frontier_loose, is_frontier_strict),
    by = "adm2_code"
  )


# メイン仕様: loose frontier サンプル
reg_dyn_frontier_loose <- reg_dyn_frontier %>%
  filter(is_frontier_loose)

# ロバストネス: strict frontier サンプル
reg_dyn_frontier_strict <- reg_dyn_frontier %>%
  filter(is_frontier_strict)

# チェック
nrow(reg_dyn_forest02)
nrow(reg_dyn_frontier_loose)
nrow(reg_dyn_frontier_strict)

reg_dyn_frontier_loose %>% count(adm1_name, sort = TRUE)
reg_dyn_frontier_strict %>% count(adm1_name, sort = TRUE)

## ステップ4 #############################################

# baseline 森林率の分布確認
summary(reg_dyn_main_islands$share_forest2000)

# 2. 中央値（median）を閾値として計算

med_share_forest2000 <- median(
  reg_dyn_main_islands$share_forest2000,
  na.rm = TRUE
)

med_share_forest2000  # 実際の値を確認しておく


# 3. 中央値以上の ADM2 のみを使うロバストネス・サンプル
#    (a) frontier まで考慮しない純粋な「高森林率サンプル」

reg_dyn_forest_median <- reg_dyn_main_islands %>%
  filter(share_forest2000 >= med_share_forest2000)

# チェック
reg_dyn_forest_median %>%
  count(adm1_name, sort = TRUE)


# 4. frontier 条件も同時に満たすロバストネス・サンプル
#    - main spec: reg_dyn_frontier_loose の中で median 以上だけ
#    - strict spec: reg_dyn_frontier_strict の中で median 以上だけ


# (a) main spec (loose frontier + median 以上の森林率)
reg_dyn_frontier_loose_med <- reg_dyn_frontier %>%
  filter(
    is_frontier_loose,
    share_forest2000 >= med_share_forest2000
  )

# (b) strict spec (strict frontier + median 以上の森林率)
reg_dyn_frontier_strict_med <- reg_dyn_frontier %>%
  filter(
    is_frontier_strict,
    share_forest2000 >= med_share_forest2000
  )

# 観測数の確認
nrow(reg_dyn_frontier_loose)
nrow(reg_dyn_frontier_loose_med)

nrow(reg_dyn_frontier_strict)
nrow(reg_dyn_frontier_strict_med)

# 州別の分布も確認
reg_dyn_frontier_loose_med %>%
  count(adm1_name, sort = TRUE)

reg_dyn_frontier_strict_med %>%
  count(adm1_name, sort = TRUE)


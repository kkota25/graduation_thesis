# --------------------------------------------------
# MODIS 土地被覆 (ADM2 × 2019-2024, IGBP クラス別)
#    → parquet 作成（動作確認付き）
# --------------------------------------------------

library(tidyverse)
library(readr)
library(arrow)

# 0. 作業ディレクトリ確認
cat("Current working directory:\n", getwd(), "\n\n")

# 1. 入力 CSV が見えているか確認
if (!file.exists("data/raw/idn_modis_lc_allclasses_adm2_2019_2024.csv")) {
  stop("data/raw/idn_modis_lc_allclasses_adm2_2019_2024.csv が見つかりません。
       getwd() が graduation_thesis プロジェクト直下になっているか確認してください。")
}

# 2. 出力先ディレクトリを作成（なければ作る）
dir.create("data/interim", showWarnings = FALSE, recursive = TRUE)

# 3. CSV 読み込み
lc_modis <- readr::read_csv(
  "data/raw/idn_modis_lc_allclasses_adm2_2019_2024.csv",
  show_col_types = FALSE
)

# 4. 型を整える（adm2_code / year を整数に）
lc_modis <- lc_modis %>%
  mutate(
    adm2_code = as.integer(adm2_code),
    year      = as.integer(year)
  )

# 5. parquet で保存
out_path <- "data/interim/modis_lc_adm2_2019_2024.parquet"

arrow::write_parquet(lc_modis, out_path)

# 6. 保存できたか確認
cat("File written? ", file.exists(out_path), "\n")


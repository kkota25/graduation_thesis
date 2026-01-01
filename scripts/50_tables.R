# 50_tables.R

# ---- Table 1: Descriptive statistics (ADM2-year) -> LaTeX (auto fit width) ----
# 2) 変数セット（必要なら列名を調整）
vars_outcome  <- c("loss_ha", "defor_rate")
vars_treat_iv <- c("ha_alerts", "ln_alerts", "cloud_share")
vars_controls <- c("chirps_mm", "burned_ha", "forest2000_ha")

# 3) 表示ラベル
var_labels <- c(
  loss_ha       = "Forest loss (ha)",
  defor_rate    = "Deforestation rate (loss / forest2000)",
  ha_alerts     = "GFW integrated alerts (ha)",
  ln_alerts     = "ln(alerts ha + 1)",
  cloud_share   = "Cloud share (annual mean)",
  chirps_mm     = "Rainfall (CHIRPS, annual total mm)",
  burned_ha     = "Burned area (ha)",
  forest2000_ha = "Baseline forest endowment (ha, year 2000)"
)

`%||%` <- function(a, b) if (!is.null(a) && !is.na(a) && nzchar(a)) a else b

summarize_var <- function(x, add_alert_extras = FALSE) {
  x_num <- suppressWarnings(as.numeric(x))
  out <- tibble(
    N      = sum(!is.na(x_num)),
    Mean   = mean(x_num, na.rm = TRUE),
    SD     = sd(x_num, na.rm = TRUE),
    Min    = min(x_num, na.rm = TRUE),
    P25    = quantile(x_num, 0.25, na.rm = TRUE, names = FALSE, type = 7),
    Median = quantile(x_num, 0.50, na.rm = TRUE, names = FALSE, type = 7),
    P75    = quantile(x_num, 0.75, na.rm = TRUE, names = FALSE, type = 7),
    Max    = max(x_num, na.rm = TRUE)
  )
  
  if (add_alert_extras) {
    out <- out %>% mutate(
      `Zero share` = mean(x_num == 0, na.rm = TRUE),
      P90 = quantile(x_num, 0.90, na.rm = TRUE, names = FALSE, type = 7),
      P95 = quantile(x_num, 0.95, na.rm = TRUE, names = FALSE, type = 7),
      P99 = quantile(x_num, 0.99, na.rm = TRUE, names = FALSE, type = 7)
    )
  } else {
    out <- out %>% mutate(`Zero share` = NA_real_, P90 = NA_real_, P95 = NA_real_, P99 = NA_real_)
  }
  out
}

make_panel <- function(reg_dt, var_vec, panel_name) {
  bind_rows(lapply(var_vec, function(v) {
    add_extras <- (v == "ha_alerts")
    summarize_var(reg_dt[[v]], add_alert_extras = add_extras) %>%
      mutate(
        Variable = var_labels[[v]] %||% v,
        panel = panel_name
      )
  }))
}

tab <- bind_rows(
  make_panel(reg_dt, vars_outcome,  "Panel A: Outcomes"),
  make_panel(reg_dt, vars_treat_iv, "Panel B: Treatment and instrument"),
  make_panel(reg_dt, vars_controls, "Panel C: Controls and baseline endowments")
) %>%
  select(panel, Variable, N, Mean, SD, Min, P25, Median, P75, Max, `Zero share`, P90, P95, P99) %>%
  mutate(
    across(c(Mean, SD, Min, P25, Median, P75, Max, P90, P95, P99), ~ round(.x, 3)),
    `Zero share` = round(`Zero share`, 3)
  )

# 4) LaTeX 出力（ここが重要：scale_downで自動的に横幅に収める）
out_path <- "../outputs/tables/table1_descriptive.tex"  # あなたのOverleaf構成に合わせる
dir.create(dirname(out_path), recursive = TRUE, showWarnings = FALSE)

tab_data <- tab %>% select(-panel)

kbl_obj <- tab_data %>%
  knitr::kable(
    format    = "latex",
    booktabs  = TRUE,
    caption   = "Summary statistics (ADM2-year panel).",
    label     = "tab:table1",          # ← \ref{tab:table1} が使える
    align     = "lrrrrrrrrrrrr",
    linesep   = ""
  ) %>%
  kableExtra::kable_styling(
    latex_options = c("hold_position", "scale_down"),  # ← はみ出し対策
    font_size     = 8                                   # 7〜9で調整
  ) %>%
  kableExtra::add_header_above(
    c(" " = 1, "Summary statistics" = 9, "Alerts distribution (only ha\\_alerts)" = 4)
  )

# panel 行を詰める
nA <- length(vars_outcome)
nB <- length(vars_treat_iv)
nC <- length(vars_controls)

kbl_obj <- kbl_obj %>%
  kableExtra::pack_rows("Panel A: Outcomes", 1, nA) %>%
  kableExtra::pack_rows("Panel B: Treatment and instrument", nA + 1, nA + nB) %>%
  kableExtra::pack_rows("Panel C: Controls and baseline endowments", nA + nB + 1, nA + nB + nC)

kableExtra::save_kable(kbl_obj, file = out_path)

out_path

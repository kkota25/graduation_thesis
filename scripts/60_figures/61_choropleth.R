# scripts/60_figures/61_choropleth.R
# ------------------------------------------------------------
# Purpose:
#   Create ADM2 × year choropleth maps (2019–2024) for:
#     1) Alert intensity        : ha_alerts  (option: ln_alerts)
#     2) Deforestation intensity: defor_rate (option: loss_ha / ln_loss)
#
# Input:
#   - data/processed/adm2_reg_2019_2024.parquet
#   - data/raw/IDN_adm2_gaul2015/*.(gpkg|shp)
#
# Output:
#   - outputs/figures/choropleth/alerts_intensity_adm2_2019_2024.(png|pdf)
#   - outputs/figures/choropleth/defor_intensity_adm2_2019_2024.(png|pdf)
#
# Run (from project root in RStudio Console):
#   source("scripts/60_figures/61_choropleth_intensity_2019_2024.R")
# ------------------------------------------------------------

suppressPackageStartupMessages({
  library(here)
  library(dplyr)
  library(arrow)
  library(sf)
  library(ggplot2)
  library(fs)
  library(classInt)
  library(viridis)
})

# -----------------------------
# Parameters
# -----------------------------
YEAR_MIN <- 2019L
YEAR_MAX <- 2024L
N_CLASS  <- 6L

# ---- choose variables here ----
# Alert intensity:
VAR_ALERT <- "ha_alerts"      # option: "ln_alerts"
# Deforestation intensity:
VAR_DEFOR <- "defor_rate"     # option: "loss_ha" or "ln_loss"

# Output directory
OUT_DIR <- here("outputs", "figures", "choropleth")
dir_create(OUT_DIR, recurse = TRUE)

# -----------------------------
# Helpers
# -----------------------------
find_adm2_geometry_file <- function(adm2_dir) {
  if (!dir_exists(adm2_dir)) {
    stop("ADM2 geometry directory not found: ", adm2_dir)
  }
  # Prefer geopackage if exists
  gpkg <- dir_ls(adm2_dir, glob = "*.gpkg", type = "file", recurse = TRUE)
  if (length(gpkg) > 0) return(gpkg[1])
  
  shp <- dir_ls(adm2_dir, glob = "*.shp", type = "file", recurse = TRUE)
  if (length(shp) > 0) return(shp[1])
  
  stop("No .gpkg or .shp found under: ", adm2_dir)
}

standardize_adm2_code_col <- function(sfobj) {
  nm <- names(sfobj)
  # common candidates
  cand <- c("ADM2_CODE", "adm2_code", "ADM2_PCODE", "adm2_pcode")
  hit <- cand[cand %in% nm]
  if (length(hit) == 0) {
    stop("ADM2 code column not found in geometry. Existing columns: ",
         paste(nm, collapse = ", "))
  }
  code_col <- hit[1]
  sfobj <- sfobj %>%
    mutate(adm2_code = as.integer(.data[[code_col]]))
  sfobj
}

make_class_breaks_safe <- function(x, n_class = 6L, style = "quantile") {
  x <- x[is.finite(x)]
  if (length(x) == 0) stop("No finite values to classify.")
  
  # classInt may fail with too many ties; handle gracefully
  brks <- tryCatch({
    ci <- classInt::classIntervals(x, n = n_class, style = style)
    ci$brks
  }, error = function(e) {
    # fallback: pretty breaks
    pretty(x, n = n_class)
  })
  
  brks <- unique(brks)
  if (length(brks) < 3) {
    # last-resort: min/max
    brks <- unique(c(min(x, na.rm = TRUE), max(x, na.rm = TRUE)))
  }
  
  # Ensure strictly increasing
  brks <- sort(brks)
  if (length(brks) >= 2 && brks[1] == brks[length(brks)]) {
    stop("Breaks collapsed (all values identical).")
  }
  brks
}

add_class_factor <- function(df, value_col, breaks) {
  # cut() requires strictly increasing breaks
  df %>%
    mutate(
      class = cut(
        .data[[value_col]],
        breaks = breaks,
        include.lowest = TRUE,
        right = TRUE
      )
    )
}

plot_facet_map <- function(sf_df, title, fill_lab) {
  ggplot(sf_df) +
    geom_sf(aes(fill = class), color = NA) +
    facet_wrap(~year, ncol = 3) +
    scale_fill_viridis_d(
      name = fill_lab,
      option = "C",
      na.value = "grey85",
      drop = FALSE
    ) +
    labs(title = title) +
    theme_minimal(base_size = 11) +
    theme(
      axis.title = element_blank(),
      axis.text = element_blank(),
      panel.grid = element_blank(),
      legend.position = "right",
      strip.text = element_text(size = 10)
    )
}

save_plot_both <- function(p, out_stem, width = 12, height = 7) {
  ggsave(file.path(OUT_DIR, paste0(out_stem, ".png")),
         plot = p, width = width, height = height, dpi = 300)
  ggsave(file.path(OUT_DIR, paste0(out_stem, ".pdf")),
         plot = p, width = width, height = height, device = cairo_pdf)
}

# -----------------------------
# 1) Load regression panel
# -----------------------------
reg_path <- here("data", "processed", "adm2_reg_2019_2024.parquet")
if (!file_exists(reg_path)) {
  stop("Input parquet not found. Run data build first: ", reg_path)
}
reg_dt <- arrow::read_parquet(reg_path) %>%
  mutate(
    year = as.integer(year),
    adm2_code = as.integer(adm2_code)
  ) %>%
  filter(year >= YEAR_MIN, year <= YEAR_MAX)

# sanity checks
req_cols <- c("adm2_code", "year", VAR_ALERT, VAR_DEFOR)
miss <- setdiff(req_cols, names(reg_dt))
if (length(miss) > 0) {
  stop("reg_dt missing required columns: ", paste(miss, collapse = ", "))
}

# -----------------------------
# 2) Load ADM2 geometry and join
# -----------------------------
adm2_dir  <- here("data", "raw", "IDN_adm2_gaul2015")
geom_path <- find_adm2_geometry_file(adm2_dir)

adm2_sf <- sf::st_read(geom_path, quiet = TRUE) %>%
  standardize_adm2_code_col()

# keep only needed geometry columns to reduce size
adm2_sf <- adm2_sf %>%
  select(adm2_code, geometry) %>%
  st_make_valid()

# Join: (ADM2 geometry) × (year panel)
map_df <- reg_dt %>%
  select(adm2_code, year, all_of(c(VAR_ALERT, VAR_DEFOR))) %>%
  left_join(adm2_sf, by = "adm2_code") %>%
  st_as_sf()

# check geometry join success
if (all(is.na(sf::st_is_empty(map_df)))) {
  stop("Geometry join seems failed: geometry is empty for all rows. ",
       "Check adm2_code consistency between parquet and geometry.")
}

# -----------------------------
# 3) Alert intensity map
# -----------------------------
alert_vals <- map_df[[VAR_ALERT]]
alert_breaks <- make_class_breaks_safe(alert_vals, n_class = N_CLASS, style = "quantile")

map_alert <- map_df %>%
  add_class_factor(VAR_ALERT, alert_breaks)

p_alert <- plot_facet_map(
  map_alert,
  title   = paste0("Alert intensity (", VAR_ALERT, ") by ADM2, ", YEAR_MIN, "–", YEAR_MAX),
  fill_lab = VAR_ALERT
)

save_plot_both(p_alert, out_stem = "alerts_intensity_adm2_2019_2024", width = 12, height = 7)

# -----------------------------
# 4) Deforestation intensity map
# -----------------------------
defor_vals <- map_df[[VAR_DEFOR]]
defor_breaks <- make_class_breaks_safe(defor_vals, n_class = N_CLASS, style = "quantile")

map_defor <- map_df %>%
  add_class_factor(VAR_DEFOR, defor_breaks)

p_defor <- plot_facet_map(
  map_defor,
  title   = paste0("Deforestation intensity (", VAR_DEFOR, ") by ADM2, ", YEAR_MIN, "–", YEAR_MAX),
  fill_lab = VAR_DEFOR
)

save_plot_both(p_defor, out_stem = "defor_intensity_adm2_2019_2024", width = 12, height = 7)

message("Done. Outputs saved under: ", OUT_DIR)
message(" - alerts_intensity_adm2_2019_2024.(png|pdf)")
message(" - defor_intensity_adm2_2019_2024.(png|pdf)")

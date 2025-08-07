# analysis.R
#
# This script performs the analysis tasks for Question 3 of the case study.
# It is designed to be run after the Python data extraction script.
#
# Tasks:
# 1. Visualize daily average wind power density maps for Europe, faceted by day.
# 2. Compute country-level average wind power density for the 3-day run.
# 3. Rank countries from highest to lowest wind power potential.

# --- 1. Setup and Package Installation ---
# List of required packages
required_packages <- c(
  "RSQLite",    # For reading from SQLite database
  "dplyr",      # For data manipulation
  "lubridate",  # For date-time manipulation
  "ggplot2",    # For plotting
  "sf",         # For handling spatial vector data (countries)
  "terra",      # For handling spatial raster data
  "rnaturalearth", # For country boundary data
  "exactextractr", # For summarizing raster data over polygons
  "viridis"     # For color palettes
)

# Function to install packages if they are not already installed
install_if_missing <- function(pkg) {
  if (!require(pkg, character.only = TRUE)) {
    install.packages(pkg, repos = "http://cran.us.r-project.org")
  }
  library(pkg, character.only = TRUE)
}

# Apply the function to all required packages
suppressPackageStartupMessages(sapply(required_packages, install_if_missing))

# --- 2. Data Loading and Preparation ---
args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 2) {
  stop("Usage: Rscript analysis.R <YYYYMMDD> <CYCLE>", call. = FALSE)
}
date_str <- args[1]
cycle_str <- args[2]

db_path <- "data/processed/gfs_data.db"
if (!file.exists(db_path)) {
  stop("Database file not found. Please run the Python data extractor first.", call. = FALSE)
}

# Connect to the SQLite database and load data
con <- RSQLite::dbConnect(RSQLite::SQLite(), db_path)
query <- sprintf(
  "SELECT lat, lon, wind_power_density, forecast_hour FROM gfs_forecasts WHERE forecast_date = '%s' AND cycle = '%s'",
  date_str, cycle_str
)
gfs_data <- RSQLite::dbGetQuery(con, query)
RSQLite::dbDisconnect(con)

if (nrow(gfs_data) == 0) {
  stop("No data found in the database for the specified date and cycle.", call. = FALSE)
}

# --- 3. Task 2b: Visualize Daily Average Wind Power Density (Faceted) ---

# Convert forecast hours into specific dates
gfs_data_daily <- gfs_data %>%
  mutate(
    run_date = as.Date(date_str, format = "%Y%m%d"),
    forecast_datetime = run_date + hours(forecast_hour),
    forecast_day = as.Date(forecast_datetime)
  )

# Calculate the average wind power density for each day and grid cell
daily_avg_wpd <- gfs_data_daily %>%
  group_by(lat, lon, forecast_day) %>%
  summarise(avg_wpd = mean(wind_power_density, na.rm = TRUE), .groups = 'drop')




# Get European country boundaries for the map overlay
europe_countries <- ne_countries(scale = "medium", returnclass = "sf", continent = "Europe")

# Create the faceted plot
wpd_map_faceted <- ggplot() +
  geom_raster(data = daily_avg_wpd, aes(x = lon, y = lat, fill = avg_wpd)) +
  geom_sf(data = europe_countries, fill = NA, color = "black", linewidth = 0.2) +
  # Use a better projection for Europe (Lambert Azimuthal Equal-Area)
  coord_sf(crs = "+proj=laea +lat_0=52 +lon_0=10 +x_0=4321000 +y_0=3210000 +ellps=GRS80 +units=m",
           xlim = c(2.5e6, 6.5e6), ylim = c(1.5e6, 5.5e6)) +
  # Use a more appropriate color scale for wind
  scale_fill_viridis_c(
    name = "Wind Power Density (W/m²)",
    option = "plasma",
    limits = c(0, max(daily_avg_wpd$avg_wpd, na.rm = TRUE)),
    na.value = "transparent"
  ) +
  # Create a separate map for each day
  facet_wrap(~ forecast_day, ncol = 3) +
  labs(
    title = paste("Daily Average Wind Power Density Forecast"),
    subtitle = paste("GFS Run:", date_str, "Cycle", cycle_str),
    x = NULL, y = NULL
  ) +
  theme_minimal() +
  theme(
    legend.position = "bottom",
    legend.key.width = unit(2, "cm"),
    plot.title = element_text(size = 16, face = "bold"),
    strip.text = element_text(size = 12, face = "bold")
  )

# Save the improved map
output_dir <- "data/processed/plots"
dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)
ggsave(
  filename = file.path(output_dir, paste0("wpd_map_faceted_", date_str, "_", cycle_str, ".png")),
  plot = wpd_map_faceted,
  width = 15,
  height = 7
)

cat("Successfully generated and saved faceted wind power density map.\n")

# --- 4. Task 3: Country Ranking by Wind Power Density ---

# First, calculate the average WPD over the entire 3-day run for each grid cell
total_avg_wpd <- gfs_data %>%
  group_by(lat, lon) %>%
  summarise(avg_wpd = mean(wind_power_density, na.rm = TRUE), .groups = 'drop')

# Convert to a raster for spatial analysis
total_wpd_raster <- terra::rast(total_avg_wpd, type = "xyz", crs = "EPSG:4326")

# Use exact_extract to get the mean WPD for each country over the full forecast period
country_avg <- exact_extract(total_wpd_raster, europe_countries, "mean")

# Combine the results with the country names and rank them
country_results <- europe_countries %>%
  select(name, iso_a3) %>%
  mutate(avg_wpd_3day = country_avg) %>%
  st_drop_geometry() %>%
  filter(!is.na(avg_wpd_3day)) %>%
  arrange(desc(avg_wpd_3day)) %>%
  mutate(rank = row_number())

# Display the results
cat("\n--- Country Wind Power Density Rankings (3-Day Average) ---\\n")
print(head(country_results, 20))

# Save the rankings to a CSV file
output_csv_path <- file.path(output_dir, paste0("country_rankings_", date_str, "_", cycle_str, ".csv"))
write.csv(country_results, output_csv_path, row.names = FALSE)
cat(paste("\nSuccessfully saved country rankings to", output_csv_path, "\n"))

# Save the rankings to the database
con <- RSQLite::dbConnect(RSQLite::SQLite(), db_path)

# --- Delete existing rankings for this forecast cycle to avoid duplicates ---
delete_query <- sprintf(
  "DELETE FROM country_rankings WHERE forecast_date = '%s' AND cycle = '%s'",
  date_str, cycle_str
)
RSQLite::dbExecute(con, delete_query)
cat(sprintf("\nDeleted existing rankings for %s cycle %s.\n", date_str, cycle_str))

db_rankings <- country_results %>%
  mutate(forecast_date = date_str, cycle = cycle_str) %>%
  rename(country = name, avg_wind_power_density = avg_wpd_3day) %>%
  select(forecast_date, cycle, country, avg_wind_power_density, rank)

# Ensure table exists and then append
if (!"country_rankings" %in% RSQLite::dbListTables(con)) {
    RSQLite::dbCreateTable(con, "country_rankings", db_rankings)
}
RSQLite::dbWriteTable(con, "country_rankings", db_rankings, append = TRUE, row.names = FALSE)
RSQLite::dbDisconnect(con)


cat("Successfully saved country rankings to the database.\n")

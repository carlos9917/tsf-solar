# analysis.R
#
# This script performs the analysis tasks for Question 3 of the case study.
# It is designed to be run after the Python data extraction script.
#
# Tasks:
# 1. Calculate Wind Power Density (already done in Python, will verify/use)
# 2. Visualize daily average wind power density maps for Europe.
# 3. Compute country-level average wind power density.
# 4. Rank countries from highest to lowest wind power potential.

# --- 1. Setup and Package Installation ---

# List of required packages
required_packages <- c(
  "ncdf4",      # For reading NetCDF files
  "dplyr",      # For data manipulation
  "ggplot2",    # For plotting
  "sf",         # For handling spatial vector data (countries)
  "terra",      # For handling spatial raster data
  "rnaturalearth", # For country boundary data
  "exactextractr" # For summarizing raster data over polygons
)

# Function to install packages if they are not already installed
install_if_missing <- function(pkg) {
  if (!require(pkg, character.only = TRUE)) {
    install.packages(pkg, repos = "http://cran.us.r-project.org")
    library(pkg, character.only = TRUE)
  }
}

# Apply the function to all required packages
sapply(required_packages, install_if_missing)

# --- 2. Data Loading ---

# This script expects the date and cycle as command-line arguments
args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 2) {
  stop("Usage: Rscript analysis.R <YYYYMMDD> <CYCLE>", call. = FALSE)
}

date_str <- args[1]
cycle_str <- args[2]

# Define the input file path based on the Python script's output
# The python script saves to a database, so we will query it.
# For simplicity in this R script, we'll assume the python script also saves a summary NetCDF or CSV.
# Let's adapt to read from the SQLite DB created by the python script.

db_path <- "data/processed/gfs_data.db"
if (!file.exists(db_path)) {
  stop("Database file not found. Please run the Python data extractor first.", call. = FALSE)
}

# Connect to the SQLite database
con <- RSQLite::dbConnect(RSQLite::SQLite(), db_path)

# Load data for the specified date and cycle
query <- sprintf(
  "SELECT lat, lon, wind_power_density, forecast_hour FROM gfs_forecasts WHERE forecast_date = '%s' AND cycle = '%s'",
  date_str, cycle_str
)

gfs_data <- RSQLite::dbGetQuery(con, query)
RSQLite::dbDisconnect(con)

if (nrow(gfs_data) == 0) {
  stop("No data found in the database for the specified date and cycle.", call. = FALSE)
}

# --- 3. Calculate Daily Average and Prepare for Visualization ---

# Calculate the average wind power density across all forecast hours for each grid cell
daily_avg_wpd <- gfs_data %>%
  group_by(lat, lon) %>%
  summarise(avg_wpd = mean(wind_power_density, na.rm = TRUE)) %>%
  ungroup()

# Convert the data frame to a SpatRaster (from terra package)
# This requires creating a raster from scattered points, which is best done by defining the grid
wpd_raster <- terra::rast(daily_avg_wpd, type = "xyz", crs = "EPSG:4326")

# --- 4. Visualize Wind Power Density Map ---

# Get European country boundaries
europe_countries <- ne_countries(scale = "medium", returnclass = "sf", continent = "Europe")

# Crop the raster to the extent of the European countries sf object for a cleaner map
europe_extent <- ext(europe_countries)
wpd_raster_cropped <- crop(wpd_raster, europe_extent)

# Create the plot
wpd_map <- ggplot() +
  geom_raster(data = as.data.frame(wpd_raster_cropped, xy = TRUE), aes(x = x, y = y, fill = avg_wpd)) +
  geom_sf(data = europe_countries, fill = NA, color = "black", size = 0.2) +
  scale_fill_viridis_c(
    name = "Wind Power Density (W/m²)",
    option = "plasma",
    limits = c(0, max(daily_avg_wpd$avg_wpd, na.rm = TRUE)),
    na.value = "transparent"
  ) +
  coord_sf(
    xlim = c(europe_extent[1], europe_extent[2]),
    ylim = c(europe_extent[3], europe_extent[4]),
    expand = FALSE
  ) +
  labs(
    title = paste("Daily Average Wind Power Density -", date_str, "Cycle", cycle_str),
    subtitle = "GFS 0.25deg Forecast",
    x = "Longitude",
    y = "Latitude"
  ) +
  theme_minimal() +
  theme(
    legend.position = "bottom",
    legend.key.width = unit(2, "cm")
  )

# Save the map
output_dir <- "data/processed/plots"
dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)
ggsave(
  filename = file.path(output_dir, paste0("wpd_map_", date_str, "_", cycle_str, ".png")),
  plot = wpd_map,
  width = 10,
  height = 10
)

cat("Successfully generated and saved wind power density map.\n")

# --- 5. Country Ranking ---

# Use exact_extract to get the mean WPD for each country
# The function 'exact_extract' takes a raster and polygons and summarizes the raster values within each polygon.
country_avg <- exact_extract(wpd_raster, europe_countries, "mean")

# Combine the results with the country names
country_results <- europe_countries %>%
  select(name, iso_a3) %>%
  mutate(avg_wpd = country_avg) %>%
  st_drop_geometry() %>%
  arrange(desc(avg_wpd)) %>%
  mutate(rank = row_number()) %>%
  filter(!is.na(avg_wpd)) # Remove countries with no data

# Display the results
cat("\n--- Country Wind Power Density Rankings ---\\n")
print(head(country_results, 20))

# Save the rankings to a CSV file
output_csv_path <- file.path(output_dir, paste0("country_rankings_", date_str, "_", cycle_str, ".csv"))
write.csv(country_results, output_csv_path, row.names = FALSE)

cat(paste("\nSuccessfully saved country rankings to", output_csv_path, "\n"))

# --- 6. Save Rankings to Database ---
con <- RSQLite::dbConnect(RSQLite::SQLite(), db_path)

db_rankings <- country_results %>%
  mutate(
    forecast_date = date_str,
    cycle = cycle_str
  ) %>%
  rename(
    country = name,
    avg_wind_power_density = avg_wpd
  ) %>%
  select(forecast_date, cycle, country, avg_wind_power_density, rank)

RSQLite::dbWriteTable(con, "country_rankings", db_rankings, append = TRUE, row.names = FALSE)

RSQLite::dbDisconnect(con)

cat("Successfully saved country rankings to the database.\n")

library(sf)
library(dplyr)
library(DBI)
library(RSQLite)
library(ggplot2)
library(ggspatial)
library(rnaturalearth)
library(rnaturalearthdata)
library(lubridate)
library(tidyr)

download_natural_earth <- function(data_dir = "data/geospatial") {
  shapefile_path <- file.path(data_dir, "ne_110m_admin_0_countries", "ne_110m_admin_0_countries.shp")
  if (file.exists(shapefile_path)) {
    return(shapefile_path)
  }
  dir.create(file.path(data_dir, "ne_110m_admin_0_countries"), recursive = TRUE, showWarnings = FALSE)
  url <- "https://naturalearth.s3.amazonaws.com/110m_cultural/ne_110m_admin_0_countries.zip"
  zip_path <- file.path(data_dir, "ne_110m_admin_0_countries.zip")
  download.file(url, zip_path, mode = "wb")
  unzip(zip_path, exdir = file.path(data_dir, "ne_110m_admin_0_countries"))
  file.remove(zip_path)
  return(shapefile_path)
}

main <- function(date_str, cycle_str) {
  db_path <- "data/processed/gfs_data.db"
  if (!file.exists(db_path)) {
    stop("Database file not found. Please run the data extractor first.")
  }
  
  con <- dbConnect(SQLite(), db_path)
  query <- sprintf("SELECT lat, lon, wind_power_density, forecast_hour FROM gfs_forecasts WHERE forecast_date = '%s' AND cycle = '%s'", date_str, cycle_str)
  gfs_data <- dbGetQuery(con, query)
  dbDisconnect(con)
  
  if (nrow(gfs_data) == 0) {
    stop("No data found in the database for the specified date and cycle.")
  }
  
  gfs_data <- gfs_data %>%
    mutate(run_date = ymd(date_str),
           forecast_datetime = run_date + hours(forecast_hour),
           forecast_day = as.Date(forecast_datetime))
  
  # Daily average wind power density
  daily_avg_wpd <- gfs_data %>%
    group_by(lat, lon, forecast_day) %>%
    summarise(wind_power_density = mean(wind_power_density, na.rm = TRUE), .groups = "drop")
  
  # Load Natural Earth shapefile
  shapefile_path <- download_natural_earth()
  world <- st_read(shapefile_path, quiet = TRUE)
  # Filter Europe countries
  #europe <- world %>% filter(continent == "Europe")
  europe <- world %>% filter(CONTINENT == "Europe")
  # Plot faceted daily average wind power density maps
  #p <- ggplot() +
  #  geom_sf(data = europe, fill = "gray90", color = "black") +
  #  coord_sf(xlim = c(-10, 40), ylim = c(35, 70), expand = FALSE) +
  #  theme_minimal() +
  #  annotation_scale(location = "bl") +
  #  annotation_north_arrow(location = "bl", which_north = "true", style = north_arrow_fancy_orienteering())

# Plot
europe_4326 <- st_transform(europe, crs = 4326)
p <- ggplot() +
  geom_raster(data = daily_avg_wpd, aes(x = lon, y = lat, fill = wind_power_density), interpolate = TRUE) +
  geom_sf(data = europe_4326, fill = NA, color = "black", size = 0.5) +  # country borders on top
  coord_sf(xlim = c(-10, 40), ylim = c(35, 70), expand = FALSE)  +
  scale_fill_viridis_c(option = "plasma", name = "Wind Power Density (W/m²)") +
  #coord_sf(xlim = c(-10, 40), ylim = c(35, 70), expand = FALSE) +
  facet_wrap(~ forecast_day) +
  labs(title = paste("Daily Average Wind Power Density (GFS Run:", date_str, "Cycle", cycle_str, ")")) +
  theme_minimal() +
  theme(
    panel.background = element_rect(fill = "white"),  # solid white background
    strip.text = element_text(size = 16),
    legend.position = "right"
  ) 
  
  # Prepare data for plotting points
  daily_avg_wpd_sf <- st_as_sf(daily_avg_wpd, coords = c("lon", "lat"), crs = 4326)
  
  # Facet by forecast_day
  #p <- p + geom_point(data = daily_avg_wpd, aes(x = lon, y = lat, color = wind_power_density), size = 1) +
  #  scale_color_viridis_c(name = "Wind Power Density (W/m²)") +
  #  facet_wrap(~ forecast_day) +
  #  labs(title = paste("Daily Average Wind Power Density (GFS Run:", date_str, "Cycle", cycle_str, ")"))

 library(viridis)  # for color scales

#p <- ggplot() +
#  geom_sf(data = europe, fill = NA, color = "black", size = 0.5) +
#  geom_raster(data = daily_avg_wpd, aes(x = lon, y = lat, fill = wind_power_density), interpolate = TRUE) +
#  scale_fill_viridis_c(option = "plasma", name = "Wind Power Density (W/m²)") +
#  coord_sf(xlim = c(-10, 40), ylim = c(35, 70), expand = FALSE) +
#  facet_wrap(~ forecast_day) +
#  labs(title = paste("Daily Average Wind Power Density (GFS Run:", date_str, "Cycle", cycle_str, ")")) +
#  theme_minimal() +
#  theme(
#    strip.text = element_text(size = 12),
#    legend.position = "right"
#  )

  
  output_dir <- "data/processed/plots"
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  plot_path <- file.path(output_dir, sprintf("wpd_map_faceted_%s_%s.png", date_str, cycle_str))
  ggsave(plot_path, p, width = 20, height = 10, units = "in")
  message("Successfully generated and saved faceted wind power density map to ", plot_path)
  
  # Country ranking
  total_avg_wpd <- gfs_data %>%
    group_by(lat, lon) %>%
    summarise(wind_power_density = mean(wind_power_density, na.rm = TRUE), .groups = "drop")
  
  points_sf <- st_as_sf(total_avg_wpd, coords = c("lon", "lat"), crs = 4326)





# Select relevant columns with correct names
countries_gdf <- europe[, c("NAME", "ISO_A3", "geometry")]
library(lwgeom)  # install.packages("lwgeom") if needed
countries_gdf <- st_make_valid(countries_gdf)

# Spatial join points with countries
joined_gdf <- st_join(points_sf, countries_gdf, join = st_within)

browser()
# Calculate average wind power density by country
country_avg <- joined_gdf %>%
  st_drop_geometry() %>%
  group_by(NAME) %>%
  summarise(avg_wpd_3day = mean(wind_power_density, na.rm = TRUE), .groups = "drop") %>%
  arrange(desc(avg_wpd_3day)) %>%
  mutate(rank = row_number())







  
  # Spatial join points with countries
  #joined <- st_join(points_sf, world[, c("name", "iso_a3", "geometry")], join = st_within)
  #
  #country_avg <- joined %>%
  #  st_drop_geometry() %>%
  #  group_by(name) %>%
  #  summarise(avg_wpd_3day = mean(wind_power_density, na.rm = TRUE), .groups = "drop") %>%
  #  arrange(desc(avg_wpd_3day)) %>%
  #  mutate(rank = row_number())
  
  message("\n--- Country Wind Power Density Rankings (3-Day Average) ---")
  print(head(country_avg, 20))
  
  csv_path <- file.path(output_dir, sprintf("country_rankings_%s_%s.csv", date_str, cycle_str))
  write.csv(country_avg, csv_path, row.names = FALSE)
  message("\nSuccessfully saved country rankings to ", csv_path)
  
  # Save rankings to database
  con <- dbConnect(SQLite(), db_path)
  dbExecute(con, "DELETE FROM country_rankings WHERE forecast_date = ? AND cycle = ?", params = list(date_str, cycle_str))
  message(sprintf("\nDeleted existing rankings for %s cycle %s.", date_str, cycle_str))
  
  db_rankings <- country_avg %>%
    rename(country = name) %>%
    mutate(forecast_date = date_str, cycle = cycle_str) %>%
    select(forecast_date, cycle, country, avg_wind_power_density = avg_wpd_3day, rank)
  
  dbWriteTable(con, "country_rankings", db_rankings, append = TRUE, row.names = FALSE)
  dbDisconnect(con)
  message("Successfully saved country rankings to the database.")
}

# To run the script from command line:
args <- commandArgs(trailingOnly = TRUE)
if (length(args) == 0) {
  # For interactive testing, set default args here
  args <- c("20250807", "12")
}

# To run the script from command line:
# args <- commandArgs(trailingOnly = TRUE)
main(args[1], args[2])

# --- Setup and Package Installation ---

# List of required packages for the script functionality:
required_packages <- c(
  "RSQLite",        # Interface to SQLite databases, used to read/write forecast data
  "dplyr",          # Data manipulation and transformation (filter, group_by, summarise, etc.)
  "lubridate",      # Simplifies working with dates and times
  "ggplot2",        # Plotting and visualization
  "sf",             # Handling spatial vector data (e.g., country polygons, points)
  "terra",          # Handling spatial raster data (e.g., gridded wind power density)
  "rnaturalearth",  # Provides Natural Earth map data (country boundaries, etc.)
  "rnaturalearthdata", # Supporting data for rnaturalearth package
  "exactextractr",  # Efficient extraction and summarization of raster values over polygons
  "viridis",        # Color palettes for plots, especially perceptually uniform scales
  "DBI",            # Database interface, used with RSQLite for database operations
  "sf",             # (Repeated) Spatial vector data handling (may be redundant here)
  "here",           # Simplifies file path management relative to project root
  "tidyr",          # Data tidying tools (pivoting, reshaping)
  "ggspatial",      # Spatial data visualization enhancements for ggplot2 (e.g., scale bars, north arrows)
  "lwgeom"          # Provides advanced spatial operations for sf, including geometry validity checks
)

# Function to install packages if they are not already installed
install_if_missing <- function(pkg) {
  if (!require(pkg, character.only = TRUE)) {
    install.packages(pkg, repos = "http://cran.us.r-project.org")
  }
  library(pkg, character.only = TRUE)
}

# Load all required packages
suppressPackageStartupMessages(sapply(required_packages, install_if_missing))


# Function to download Natural Earth shapefile if not already present
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


# Main function to process data, generate plots, and save rankings
main <- function(date_str, cycle_str) {
  db_path <- "data/processed/gfs_data.db"
  
  # Check if database exists
  if (!file.exists(db_path)) {
    stop("Database file not found. Please run the data extractor first.")
  }
  
  # Connect to SQLite database and query GFS forecast data for given date and cycle
  con <- dbConnect(SQLite(), db_path)
  query <- sprintf("SELECT lat, lon, wind_power_density, forecast_hour FROM gfs_forecasts WHERE forecast_date = '%s' AND cycle = '%s'", date_str, cycle_str)
  gfs_data <- dbGetQuery(con, query)
  dbDisconnect(con)
  
  # Stop if no data found
  if (nrow(gfs_data) == 0) {
    stop("No data found in the database for the specified date and cycle.")
  }
  
  # Add run date and forecast datetime columns
  gfs_data <- gfs_data %>%
    mutate(run_date = ymd(date_str),
           forecast_datetime = run_date + hours(forecast_hour),
           forecast_day = as.Date(forecast_datetime))
  
  # Calculate daily average wind power density by lat/lon
  daily_avg_wpd <- gfs_data %>%
    group_by(lat, lon, forecast_day) %>%
    summarise(wind_power_density = mean(wind_power_density, na.rm = TRUE), .groups = "drop")
  
  # Load Natural Earth shapefile for country boundaries
  shapefile_path <- download_natural_earth()
  world <- st_read(shapefile_path, quiet = TRUE)
  
  # Filter for European countries only
  europe <- world %>% filter(CONTINENT == "Europe")
  
  # Transform Europe shapefile to WGS84 coordinate system (EPSG:4326)
  europe_4326 <- st_transform(europe, crs = 4326)
  
  # Generate faceted raster plot of daily average wind power density over Europe
  p <- ggplot() +
    geom_raster(data = daily_avg_wpd, aes(x = lon, y = lat, fill = wind_power_density), interpolate = TRUE) +
    geom_sf(data = europe_4326, fill = NA, color = "black", size = 0.5) +  # country borders on top
    coord_sf(xlim = c(-10, 40), ylim = c(35, 70), expand = FALSE) +
    scale_fill_viridis_c(option = "plasma", name = "Wind Power Density (W/m²)") +
    facet_wrap(~ forecast_day) +
    labs(title = paste("Daily Average Wind Power Density (GFS Run:", date_str, "Cycle", cycle_str, ")")) +
    theme_minimal() +
    theme(
      panel.background = element_rect(fill = "white"),  # solid white background
      strip.text = element_text(size = 16),
      legend.position = "right"
    )
  
  # Create output directory for plots if it doesn't exist
  output_dir <- "data/processed/plots"
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  
  # Save the plot to file
  plot_path <- file.path(output_dir, sprintf("wpd_map_faceted_%s_%s.png", date_str, cycle_str))
  ggsave(plot_path, p, width = 20, height = 10, units = "in")
  message("Successfully generated and saved faceted wind power density map to ", plot_path)
  
  # Calculate total average wind power density by lat/lon (across all forecast days)
  total_avg_wpd <- gfs_data %>%
    group_by(lat, lon) %>%
    summarise(wind_power_density = mean(wind_power_density, na.rm = TRUE), .groups = "drop")
  
  # Convert points to spatial features
  points_sf <- st_as_sf(total_avg_wpd, coords = c("lon", "lat"), crs = 4326)
  
  # Select relevant columns from Europe shapefile and ensure valid geometries
  countries_gdf <- europe[, c("NAME", "ISO_A3", "geometry")]
  countries_gdf <- st_make_valid(countries_gdf)  # fix any invalid polygons
  
  # Spatial join: assign each point to the country polygon it falls within
  joined_gdf <- st_join(points_sf, countries_gdf, join = st_within)
  
  # Calculate average wind power density by country
  country_avg <- joined_gdf %>%
    st_drop_geometry() %>%
    group_by(NAME) %>%
    summarise(avg_wpd_3day = mean(wind_power_density, na.rm = TRUE), .groups = "drop") %>%
    arrange(desc(avg_wpd_3day)) %>%
    mutate(rank = row_number())
  
  # Print top 20 countries by average wind power density
  message("\n--- Country Wind Power Density Rankings (3-Day Average) ---")
  print(head(country_avg, 20))
  
  # Save country rankings to CSV file
  csv_path <- file.path(output_dir, sprintf("country_rankings_%s_%s.csv", date_str, cycle_str))
  write.csv(country_avg, csv_path, row.names = FALSE)
  message("\nSuccessfully saved country rankings to ", csv_path)
  
  # Save rankings to database
  con <- dbConnect(SQLite(), db_path)
  
  # Delete existing rankings for the given date and cycle to avoid duplicates
  dbExecute(con, "DELETE FROM country_rankings WHERE forecast_date = ? AND cycle = ?", params = list(date_str, cycle_str))
  message(sprintf("\nDeleted existing rankings for %s cycle %s.", date_str, cycle_str))
  
  # Rename 'NAME' column to 'country' to match database schema
  # Add forecast_date and cycle columns
  # Select and rename columns to match database table structure
  db_rankings <- country_avg %>%
    rename(country = NAME) %>%  # Fixed: use 'NAME' instead of 'name'
    mutate(forecast_date = date_str, cycle = cycle_str) %>%
    select(forecast_date, cycle, country, avg_wind_power_density = avg_wpd_3day, rank)
  
  # Write the updated rankings to the database
  dbWriteTable(con, "country_rankings", db_rankings, append = TRUE, row.names = FALSE)
  dbDisconnect(con)
  message("Successfully saved country rankings to the database.")
}


# Run the script from command line or interactive session
args <- commandArgs(trailingOnly = TRUE)
if (length(args) == 0) {
  # Default arguments for interactive testing
  args <- c("20250807", "12")
}

main(args[1], args[2])

# dashboard.R

# --- 1. Setup and Package Installation ---

# List of required packages for the dashboard
required_packages <- c(
  "shiny", "shinydashboard", "leaflet", "plotly", "DBI", "RSQLite", 
  "dplyr", "ggplot2", "sf", "rnaturalearth", "rnaturalearthdata", "tidyr",
  "viridis"
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

# --- 2. Database Connection and Data Functions ---

# Define database path
db_path <- "data/processed/gfs_data.db"

# Function to get available forecast dates from the database
get_available_dates <- function() {
  con <- dbConnect(SQLite(), db_path)
  dates <- dbGetQuery(con, "SELECT DISTINCT forecast_date FROM gfs_forecasts ORDER BY forecast_date DESC")
  dbDisconnect(con)
  return(dates$forecast_date)
}

# Function to get available cycles for a specific date
get_available_cycles <- function(date) {
  con <- dbConnect(SQLite(), db_path)
  query <- sprintf("SELECT DISTINCT cycle FROM gfs_forecasts WHERE forecast_date = '%s' ORDER BY cycle", date)
  cycles <- dbGetQuery(con, query)
  dbDisconnect(con)
  return(cycles$cycle)
}

# Function to load forecast data for a specific date and cycle
load_forecast_data <- function(date, cycle) {
  con <- dbConnect(SQLite(), db_path)
  query <- sprintf("SELECT * FROM gfs_forecasts WHERE forecast_date = '%s' AND cycle = '%s'", date, cycle)
  df <- dbGetQuery(con, query)
  dbDisconnect(con)
  return(df)
}

# Function to load country rankings for a specific date and cycle
load_country_rankings <- function(date, cycle) {
  con <- dbConnect(SQLite(), db_path)
  query <- sprintf("SELECT * FROM country_rankings WHERE forecast_date = '%s' AND cycle = '%s' ORDER BY rank", date, cycle)
  df <- dbGetQuery(con, query)
  dbDisconnect(con)
  return(df)
}


# --- 3. Shiny UI Definition ---

ui <- dashboardPage(
  dashboardHeader(title = "GFS Wind Power Dashboard"),
  dashboardSidebar(
    selectInput("date_selector", "Select Date", choices = get_available_dates()),
    selectInput("cycle_selector", "Select Cycle", choices = NULL),
    sliderInput("hour_slider", "Forecast Hour", min = 0, max = 72, value = 0, step = 3)
  ),
  dashboardBody(
    fluidRow(
      box(title = "Wind Power Density Map", width = 12, solidHeader = TRUE, status = "primary",
          leafletOutput("wind_power_map", height = 600))
    ),
    fluidRow(
      box(title = "Country Rankings", width = 6, solidHeader = TRUE, status = "info",
          plotlyOutput("country_rankings_chart")),
      box(title = "Average Wind Power Density", width = 6, solidHeader = TRUE, status = "info",
          plotlyOutput("time_series_chart"))
    )
  )
)


# --- 4. Shiny Server Definition ---

server <- function(input, output, session) {
  
  # Update cycle selector based on date selection
  observe({
    req(input$date_selector)
    cycles <- get_available_cycles(input$date_selector)
    updateSelectInput(session, "cycle_selector", choices = cycles, selected = cycles[1])
  })
  
  # Reactive expression to load forecast data
  forecast_data <- reactive({
    req(input$date_selector, input$cycle_selector)
    load_forecast_data(input$date_selector, input$cycle_selector)
  })
  
  # Reactive expression to load country rankings
  country_data <- reactive({
    req(input$date_selector, input$cycle_selector)
    load_country_rankings(input$date_selector, input$cycle_selector)
  })
  
  # Render Leaflet map
  output$wind_power_map <- renderLeaflet({
    df <- forecast_data()
    df_hour <- df %>% filter(forecast_hour == input$hour_slider)
    
    if (nrow(df_hour) == 0) return(leaflet() %>% addTiles())
    
    #pal <- colorNumeric(palette = "viridis", domain = df_hour$wind_power_density)

    #pal <- colorNumeric(palette = viridis(256, direction = -1), domain = df_hour$wind_power_density)
    pal <- colorNumeric(palette = viridis(256), domain = df_hour$wind_power_density)

    
    leaflet(df_hour) %>%
      addTiles() %>%
      addCircleMarkers(
        lng = ~lon, lat = ~lat,
        radius = ~sqrt(wind_power_density / max(wind_power_density, na.rm = TRUE) * 100),
        color = ~pal(wind_power_density),
        stroke = FALSE, fillOpacity = 0.7,
        popup = ~paste0("WPD: ", round(wind_power_density, 2), " W/m²")
      ) %>%
      #addLegend("bottomleft", pal = pal, values = ~wind_power_density,
      #          title = "Wind Power Density",
      #          opacity = 1) %>%
      #addLegend("bottomright", pal = pal, values = rev(sort(df_hour$wind_power_density)),
      #    title = "Wind Power Density",
      #    opacity = 1) %>%
     addLegend("bottomleft", pal = pal, values = df_hour$wind_power_density,
          title = "Wind Power Density",
          opacity = 1) %>%
      setView(lng = 15, lat = 55, zoom = 4)
  })
  
  # Render country rankings bar chart
  output$country_rankings_chart <- renderPlotly({
    df <- country_data()
    if (nrow(df) == 0) return(NULL)
    
    p <- ggplot(df, aes(x = reorder(country, avg_wind_power_density), y = avg_wind_power_density)) +
      geom_bar(stat = "identity", fill = "skyblue") +
      coord_flip() +
      labs(x = "Country", y = "Average Wind Power Density (W/m²)", title = "Country Rankings") +
      theme_minimal()
      
    ggplotly(p)
  })
  
  # Render time series chart
  output$time_series_chart <- renderPlotly({
    df <- forecast_data()
    if (nrow(df) == 0) return(NULL)
    
    hourly_avg <- df %>%
      group_by(forecast_hour) %>%
      summarise(avg_wpd = mean(wind_power_density, na.rm = TRUE))
      
    p <- ggplot(hourly_avg, aes(x = forecast_hour, y = avg_wpd)) +
      geom_line(color = "steelblue") +
      geom_point(color = "steelblue") +
      labs(x = "Forecast Hour", y = "Average Wind Power Density (W/m²)", title = "Forecast Time Series") +
      theme_minimal()
      
    ggplotly(p)
  })
}

# --- 5. Run the Shiny App ---

# Use a specific host and port for consistency
shinyApp(ui, server, options = list(host = '0.0.0.0', port = 8050))

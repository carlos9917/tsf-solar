# GFS data extractor
"""
GFS Data Extractor Module 
Handles downloading and processing of NOAA GFS forecast data using correct NOMADS URLs
"""

import os
import sqlite3
import pandas as pd
import numpy as np
import xarray as xr
import requests
from datetime import datetime, timedelta
from loguru import logger
import dask.array as da
from pathlib import Path
import sys
sys.path.append('config')
from config import *

class GFSDataExtractor:
    def __init__(self):
        self.setup_directories()
        self.setup_logging()
        self.setup_database()
        
    def setup_directories(self):
        """Ensure all required directories exist"""
        directories = ['data/raw', 'data/processed', 'logs', 'config', 'src']
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)
        
    def setup_logging(self):
        """Setup logging with loguru"""
        logger.add(LOG_FILE, rotation="10 MB", level=LOG_LEVEL)
        logger.info("GFS Data Extractor initialized")
        
    def setup_database(self):
        """Initialize SQLite database"""
        try:
            conn = sqlite3.connect(DATABASE_PATH)
            cursor = conn.cursor()
            
            # Create tables
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS gfs_forecasts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    forecast_date TEXT,
                    cycle TEXT,
                    forecast_hour INTEGER,
                    lat REAL,
                    lon REAL,
                    u_wind_100m REAL,
                    v_wind_100m REAL,
                    temp_2m REAL,
                    wind_power_density REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS country_rankings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    forecast_date TEXT,
                    cycle TEXT,
                    country TEXT,
                    avg_wind_power_density REAL,
                    rank INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.commit()
            conn.close()
            logger.info("Database initialized successfully")
            
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            
    def check_file_availability(self, date_str, cycle, forecast_hour):
        """Check if a specific GFS file is available"""
        url = build_gfs_url(date_str, cycle, forecast_hour)
        try:
            response = requests.head(url, timeout=10)
            return response.status_code == 200
        except:
            return False
            
    def download_gfs_file(self, date_str, cycle, forecast_hour):
        """Download GFS file using specified method"""
        return self.download_direct(date_str, cycle, forecast_hour)
            
    def download_direct(self, date_str, cycle, forecast_hour):
        """Download GFS file directly from NOMADS"""
        url = build_gfs_url(date_str, cycle, forecast_hour)
        logger.info(f"Downloading: {url}")
        
        try:
            response = requests.get(url, timeout=300)
            response.raise_for_status()
            
            # Save to temporary file
            temp_file = f"data/raw/gfs_{date_str}_{cycle}_{forecast_hour:03d}.grb2"
            with open(temp_file, 'wb') as f:
                f.write(response.content)
                
            logger.info(f"Downloaded {len(response.content)} bytes to {temp_file}")
            return temp_file
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download {url}: {e}")
            return None
            
    def download_grib_filter(self, date_str, cycle, forecast_hour):
        """Download GFS data using GRIB filter service"""
        url = build_grib_filter_url(date_str, cycle, forecast_hour)
        logger.info(f"Downloading via GRIB filter: forecast hour {forecast_hour}")
        
        try:
            response = requests.get(url, timeout=300)
            response.raise_for_status()
            
            # Save to temporary file
            temp_file = f"data/raw/gfs_filtered_{date_str}_{cycle}_{forecast_hour:03d}.grb2"
            with open(temp_file, 'wb') as f:
                f.write(response.content)
                
            logger.info(f"Downloaded filtered data: {len(response.content)} bytes")
            return temp_file
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download via GRIB filter: {e}")
            return None
            
    def process_grib_file(self, file_path, date_str, cycle, forecast_hour):
        """Process a single GRIB2 file"""
        try:
            logger.info(f"Processing GRIB file: {file_path}")
            
            # Load with xarray/cfgrib
            ds = xr.open_dataset(file_path, engine='cfgrib')
            
            # Subset for European region
            ds_subset = ds.sel(
                latitude=slice(EUROPE_BOUNDS['lat_max'], EUROPE_BOUNDS['lat_min']),
                longitude=slice(EUROPE_BOUNDS['lon_min'], EUROPE_BOUNDS['lon_max'])
            )
            
            # Extract variables (handle different naming conventions)
            variables = {}
            
            # Look for wind components at 100m
            for var_name in ds_subset.data_vars:
                attrs = ds_subset[var_name].attrs
                
                if ('u-component of wind' in str(attrs).lower() and 
                    '100 m above ground' in str(attrs).lower()):
                    variables['u_wind_100m'] = ds_subset[var_name]
                elif ('v-component of wind' in str(attrs).lower() and 
                      '100 m above ground' in str(attrs).lower()):
                    variables['v_wind_100m'] = ds_subset[var_name]
                elif ('temperature' in str(attrs).lower() and 
                      '2 m above ground' in str(attrs).lower()):
                    variables['temp_2m'] = ds_subset[var_name]
                    
            # Alternative: try by variable name patterns
            if len(variables) < 3:
                for var_name in ds_subset.data_vars:
                    if 'u' in var_name.lower() and ('100' in var_name or 'wind' in var_name.lower()):
                        variables['u_wind_100m'] = ds_subset[var_name]
                    elif 'v' in var_name.lower() and ('100' in var_name or 'wind' in var_name.lower()):
                        variables['v_wind_100m'] = ds_subset[var_name]
                    elif 't' in var_name.lower() and ('2m' in var_name.lower() or 'temp' in var_name.lower()):
                        variables['temp_2m'] = ds_subset[var_name]
            
            if len(variables) < 2:  # At least need wind components
                logger.warning(f"Could not find required variables. Available: {list(ds_subset.data_vars.keys())}")
                return None
                
            # Calculate wind power density if we have wind components
            if 'u_wind_100m' in variables and 'v_wind_100m' in variables:
                u_wind = variables['u_wind_100m']
                v_wind = variables['v_wind_100m']
                wind_speed = np.sqrt(u_wind**2 + v_wind**2)
                
                # Air density (kg/m³) - approximate for standard conditions
                air_density = 1.225
                
                # Wind power density (W/m²) = 0.5 * ρ * v³
                wind_power_density = 0.5 * air_density * wind_speed**3
                variables['wind_power_density'] = wind_power_density
            
            # Convert to DataFrame
            df_data = []
            
            for lat_idx, lat in enumerate(ds_subset.latitude.values):
                for lon_idx, lon in enumerate(ds_subset.longitude.values):
                    row = {
                        'forecast_date': date_str,
                        'cycle': cycle,
                        'forecast_hour': forecast_hour,
                        'lat': float(lat),
                        'lon': float(lon)
                    }
                    
                    # Extract values for this location
                    for var_name, var_data in variables.items():
                        if len(var_data.dims) == 2:  # lat, lon
                            value = var_data.isel(latitude=lat_idx, longitude=lon_idx).values
                        else:  # might have time dimension
                            value = var_data.isel(latitude=lat_idx, longitude=lon_idx).values
                            if hasattr(value, 'item'):
                                value = value.item()
                        
                        row[var_name] = float(value) if not np.isnan(value) else None
                    
                    df_data.append(row)
            
            df = pd.DataFrame(df_data)
            df = df.dropna()  # Remove rows with missing data
            
            logger.info(f"Processed {len(df)} data points from {file_path}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to process GRIB file {file_path}: {e}")
            return None
        finally:
            # Clean up temporary file
            if os.path.exists(file_path):
                os.remove(file_path)
                
    def save_to_database(self, df):
        """Save processed data to SQLite database"""
        try:
            conn = sqlite3.connect(DATABASE_PATH)
            
            # Select relevant columns for database
            db_columns = [
                'forecast_date', 'cycle', 'forecast_hour', 'lat', 'lon',
                'u_wind_100m', 'v_wind_100m', 'temp_2m', 'wind_power_density'
            ]
            
            # Only keep columns that exist in the dataframe
            available_columns = [col for col in db_columns if col in df.columns]
            df_db = df[available_columns]
            
            # Insert data
            df_db.to_sql('gfs_forecasts', conn, if_exists='append', index=False)
            
            conn.close()
            logger.info(f"Saved {len(df_db)} records to database")
            
        except Exception as e:
            logger.error(f"Failed to save data to database: {e}")
            
    def calculate_country_rankings(self, date_str, cycle):
        """Calculate country-level wind power density rankings"""
        try:
            conn = sqlite3.connect(DATABASE_PATH)
            
            # Query data for this forecast
            query = """
                SELECT lat, lon, wind_power_density 
                FROM gfs_forecasts 
                WHERE forecast_date = ? AND cycle = ?
            """
            
            df = pd.read_sql_query(query, conn, params=(date_str, cycle))
            
            if df.empty:
                logger.warning("No data found for country rankings calculation")
                return
            
            # Simple country assignment based on lat/lon
            countries = []
            for _, row in df.iterrows():
                lat, lon = row['lat'], row['lon']
                if lat > 60:
                    country = 'Norway'
                elif lat > 55 and lon < 15:
                    country = 'Denmark'
                elif lat > 50 and lon < 5:
                    country = 'United Kingdom'
                elif lat > 45 and lon < 10:
                    country = 'France'
                elif lat > 45 and lon > 10:
                    country = 'Germany'
                elif lat > 40 and lon > 20:
                    country = 'Eastern Europe'
                else:
                    country = 'Other'
                countries.append(country)
            
            df['country'] = countries
            
            # Calculate country averages
            country_avg = df.groupby('country')['wind_power_density'].mean().reset_index()
            country_avg = country_avg.sort_values('wind_power_density', ascending=False)
            country_avg['rank'] = range(1, len(country_avg) + 1)
            country_avg['forecast_date'] = date_str
            country_avg['cycle'] = cycle
            
            # Rename column
            country_avg = country_avg.rename(columns={
                'wind_power_density': 'avg_wind_power_density'
            })
            
            # Save to database
            country_avg.to_sql('country_rankings', conn, if_exists='append', index=False)
            
            conn.close()
            logger.info(f"Calculated rankings for {len(country_avg)} countries")
            
        except Exception as e:
            logger.error(f"Failed to calculate country rankings: {e}")
            
    def run_extraction(self, date_str=None, cycle=None):
        """Run complete data extraction pipeline"""
        if date_str is None:
            date_str = get_latest_available_date()
            logger.info(f"Using latest available date: {date_str}")
        
        if cycle is None:
            available_cycles = get_available_cycles(date_str)
            cycles_to_process = available_cycles
        else:
            cycles_to_process = [cycle]
        
        logger.info(f"Processing cycles: {cycles_to_process} for date {date_str}")
        
        for cycle in cycles_to_process:
            logger.info(f"Processing cycle {cycle} for date {date_str}")
            
            cycle_data = []
            
            # Process each forecast hour
            for forecast_hour in FORECAST_HOURS:
                logger.info(f"Processing forecast hour {forecast_hour}")
                
                # Try direct download first, then GRIB filter
                file_path = None
                file_path = self.download_gfs_file(date_str, cycle, forecast_hour)
                if file_path:
                    break
                
                if not file_path:
                    logger.warning(f"Could not download data for {date_str} cycle {cycle} hour {forecast_hour}")
                    continue
                
                # Process the downloaded file
                df = self.process_grib_file(file_path, date_str, cycle, forecast_hour)
                if df is not None:
                    cycle_data.append(df)
                else:
                    logger.warning(f"Could not process data for {date_str} cycle {cycle} hour {forecast_hour}")
            
            # Combine all forecast hours for this cycle
            if cycle_data:
                combined_df = pd.concat(cycle_data, ignore_index=True)
                
                # Save to database
                self.save_to_database(combined_df)
                
                # Calculate country rankings
                self.calculate_country_rankings(date_str, cycle)
                
                logger.info(f"Completed processing for {date_str} cycle {cycle}: {len(combined_df)} total records")
            else:
                logger.error(f"No data processed for {date_str} cycle {cycle}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='GFS Data Extractor')
    parser.add_argument('--date', help='Date for extraction (YYYYMMDD)')
    parser.add_argument('--cycle', choices=['00', '06', '12', '18'], 
                       help='GFS cycle for extraction')
    
    args = parser.parse_args()
    
    extractor = GFSDataExtractor()
    extractor.run_extraction(args.date, args.cycle)

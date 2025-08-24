import duckdb
import pandas as pd
from scipy.spatial import cKDTree
import numpy as np
from loguru import logger
import sys
sys.path.append('config')
from config import DATABASE_PATH

def calculate_power_at_locations():
    """
    Calculates wind power density at specific locations from the GFS forecast data.
    """
    try:
        # Connect to DuckDB
        conn = duckdb.connect(DATABASE_PATH)

        # Load wind power plant coordinates
        plant_coords = pd.read_csv('/home/tenantadmin/tsf-solar/wind_power_plants_coordinates.csv')
        plant_coords = plant_coords[['lat', 'lon']].dropna().drop_duplicates()

        # Load GFS forecast data
        gfs_data = conn.execute("SELECT DISTINCT lat, lon, forecast_date, cycle, forecast_hour, wind_power_density FROM gfs_forecasts").fetchdf()

        if gfs_data.empty:
            logger.warning("No GFS data found in the database.")
            return

        # Create a KDTree for fast nearest neighbor search
        gfs_unique_coords = gfs_data[['lat', 'lon']].drop_duplicates()
        tree = cKDTree(gfs_unique_coords[['lat', 'lon']].values)

        # Find the index of the nearest GFS point for each power plant
        distances, indices = tree.query(plant_coords[['lat', 'lon']].values, k=1)
        
        # Get the coordinates of the nearest GFS points
        nearest_gfs_coords = gfs_unique_coords.iloc[indices]

        # Create a DataFrame for merging
        plant_coords['merge_lat'] = nearest_gfs_coords['lat'].values
        plant_coords['merge_lon'] = nearest_gfs_coords['lon'].values

        # Merge with GFS data
        plant_forecasts = pd.merge(
            gfs_data,
            plant_coords,
            left_on=['lat', 'lon'],
            right_on=['merge_lat', 'merge_lon']
        )

        # Create a new table for power plant forecasts
        conn.execute("""
            CREATE TABLE IF NOT EXISTS wind_power_plant_forecasts (
                forecast_date TEXT,
                cycle TEXT,
                forecast_hour INTEGER,
                lat DOUBLE,
                lon DOUBLE,
                wind_power_density DOUBLE
            )
        """)

        # Save to the new table
        conn.append('wind_power_plant_forecasts', plant_forecasts[['forecast_date', 'cycle', 'forecast_hour', 'lat_y', 'lon_y', 'wind_power_density']].rename(columns={'lat_y': 'lat', 'lon_y': 'lon'}))
        
        logger.info(f"Saved {len(plant_forecasts)} records to wind_power_plant_forecasts table.")

        conn.close()

    except Exception as e:
        logger.error(f"Failed to calculate power at locations: {e}")

if __name__ == "__main__":
    calculate_power_at_locations()
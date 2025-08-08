# src/analysis.py
import fiona
import geopandas as gpd
world = gpd.read_file("data/geospatial/ne_110m_admin_0_countries/ne_110m_admin_0_countries.shp")

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import numpy as np
from shapely.geometry import Point
import argparse
import os
from pathlib import Path
import requests
import zipfile

def download_natural_earth(data_dir="data/geospatial"):
    """Downloads and extracts the Natural Earth dataset if not present."""
    data_dir = Path(data_dir)
    shapefile_path = data_dir / "ne_110m_admin_0_countries" / "ne_110m_admin_0_countries.shp"

    if shapefile_path.exists():
        return shapefile_path

    print("Downloading Natural Earth dataset...")
    data_dir.mkdir(parents=True, exist_ok=True)
    
    url = "https://naturalearth.s3.amazonaws.com/110m_cultural/ne_110m_admin_0_countries.zip"
    zip_path = data_dir / "ne_110m_admin_0_countries.zip"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
                      " Chrome/58.0.3029.110 Safari/537.3"
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        with open(zip_path, 'wb') as f:
            f.write(response.content)

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(data_dir / "ne_110m_admin_0_countries")
        
        zip_path.unlink()  # Clean up the zip file
        print("downloaded and extracted Natural Earth dataset.")
        return shapefile_path
    except Exception as e:
        print(f"Failed to download or process Natural Earth dataset: {e}")
        return None
def main(date_str, cycle_str):
    """Main function to perform analysis."""
    db_path = "data/processed/gfs_data.db"
    if not os.path.exists(db_path):
        print("Database file not found. Please run the data extractor first.")
        return

    # --- 1. Data Loading ---
    con = sqlite3.connect(db_path)
    query = f"SELECT lat, lon, wind_power_density, forecast_hour FROM gfs_forecasts WHERE forecast_date = '{date_str}' AND cycle = '{cycle_str}'"
    gfs_data = pd.read_sql_query(query, con)
    con.close()

    if gfs_data.empty:
        print("No data found in the database for the specified date and cycle.")
        return

    # --- 2. Data Preparation ---
    gfs_data['run_date'] = pd.to_datetime(date_str, format='%Y%m%d')
    gfs_data['forecast_datetime'] = gfs_data['run_date'] + pd.to_timedelta(gfs_data['forecast_hour'], unit='h')
    gfs_data['forecast_day'] = gfs_data['forecast_datetime'].dt.date

    # --- 3. Visualize Daily Average Wind Power Density ---
    daily_avg_wpd = gfs_data.groupby(['lat', 'lon', 'forecast_day'])['wind_power_density'].mean().reset_index()

    # Get European country boundaries
    #world_shapefile = download_natural_earth()
    #if world_shapefile is None:
    #    print("Could not load map data. Aborting visualization.")
    #    return
    #world = gpd.read_file("data/geospatial/ne_110m_admin_0_countries/ne_110m_admin_0_countries.shp")
    #world = gpd.read_file("data/geospatial/ne_110m_admin_0_countries/ne_110m_admin_0_countries.shp")
    #europe = world[world.continent == 'Europe']

    # Create faceted plot
    unique_days = sorted(daily_avg_wpd['forecast_day'].unique())
    proj = ccrs.LambertConformal(central_longitude=10.0, central_latitude=52.0)
    fig, axes = plt.subplots(1, len(unique_days), figsize=(20, 10), subplot_kw={'projection': proj})
    if len(unique_days) == 1:
        axes = [axes]

    for i, day in enumerate(unique_days):
        ax = axes[i]
        day_data = daily_avg_wpd[daily_avg_wpd['forecast_day'] == day]

        ax.set_extent([-10, 40, 35, 70], crs=ccrs.PlateCarree())
        ax.add_feature(cfeature.COASTLINE)
        ax.add_feature(cfeature.BORDERS, linestyle=':')
        ax.add_feature(cfeature.LAND, edgecolor='black')
        ax.add_feature(cfeature.OCEAN)
        ax.gridlines(draw_labels=True, dms=True, x_inline=False, y_inline=False)

        contour = ax.tricontourf(day_data['lon'], day_data['lat'], day_data['wind_power_density'],
                                 transform=ccrs.PlateCarree(), cmap='viridis', levels=15)

        ax.set_title(f"Forecast Day: {day.strftime('%Y-%m-%d')}")

    fig.colorbar(contour, ax=axes, orientation='horizontal', fraction=0.05, pad=0.1, label="Wind Power Density (W/m²)")
    plt.suptitle(f"Daily Average Wind Power Density (GFS Run: {date_str} Cycle {cycle_str})", fontsize=16)
    plt.tight_layout(rect=[0, 0.05, 1, 0.96])

    output_dir = Path("data/processed/plots")
    output_dir.mkdir(parents=True, exist_ok=True)
    plot_path = output_dir / f"wpd_map_faceted_{date_str}_{cycle_str}.png"
    plt.savefig(plot_path)
    print(f"Successfully generated and saved faceted wind power density map to {plot_path}")

    # --- 4. Country Ranking ---
    total_avg_wpd = gfs_data.groupby(['lat', 'lon'])['wind_power_density'].mean().reset_index()
    world = gpd.read_file("data/geospatial/ne_110m_admin_0_countries/ne_110m_admin_0_countries.shp")
    
    geometry = [Point(xy) for xy in zip(total_avg_wpd.lon, total_avg_wpd.lat)]
    points_gdf = gpd.GeoDataFrame(total_avg_wpd, crs="EPSG:4326", geometry=geometry)

    countries_gdf = world[['name', 'iso_a3', 'geometry']]
    joined_gdf = gpd.sjoin(points_gdf, countries_gdf, how="inner", predicate='within')

    country_avg = joined_gdf.groupby('name')['wind_power_density'].mean().reset_index()
    country_results = country_avg.sort_values('wind_power_density', ascending=False).reset_index(drop=True)
    country_results['rank'] = country_results.index + 1
    country_results = country_results.rename(columns={'wind_power_density': 'avg_wpd_3day'})

    print("\n--- Country Wind Power Density Rankings (3-Day Average) ---")
    print(country_results.head(20))

    csv_path = output_dir / f"country_rankings_{date_str}_{cycle_str}.csv"
    country_results.to_csv(csv_path, index=False)
    print(f"\nSuccessfully saved country rankings to {csv_path}")

    # --- 5. Save to Database ---
    con = sqlite3.connect(db_path)
    cursor = con.cursor()

    delete_query = "DELETE FROM country_rankings WHERE forecast_date = ? AND cycle = ?"
    cursor.execute(delete_query, (date_str, cycle_str))
    print(f"\nDeleted existing rankings for {date_str} cycle {cycle_str}.")

    db_rankings = country_results.rename(columns={'name': 'country', 'avg_wpd_3day': 'avg_wind_power_density'})
    db_rankings['forecast_date'] = date_str
    db_rankings['cycle'] = cycle_str
    db_rankings[['forecast_date', 'cycle', 'country', 'avg_wind_power_density', 'rank']].to_sql('country_rankings', con, if_exists='append', index=False)
    
    con.commit()
    con.close()
    print("Successfully saved country rankings to the database.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Perform analysis on GFS data.")
    parser.add_argument("date", help="Date for the analysis in YYYYMMDD format.")
    parser.add_argument("cycle", help="Cycle for the analysis (e.g., 06).")
    args = parser.parse_args()
    main(args.date, args.cycle)

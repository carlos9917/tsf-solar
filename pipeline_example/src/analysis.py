# src/analysis.py

import sqlite3
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import contextily as cx
from rasterio.transform import from_origin
from rasterio.features import rasterize
import numpy as np
from shapely.geometry import Point
import argparse
import os
from pathlib import Path

def create_raster_from_df(df, lon_col='lon', lat_col='lat', val_col='avg_wpd'):
    """Creates a raster-like numpy array and transform from a pandas DataFrame."""
    # Define the spatial resolution
    lon_res = df[lon_col].diff().abs().median()
    lat_res = df[lat_col].diff().abs().median()

    # Create the transform
    transform = from_origin(
        df[lon_col].min() - lon_res / 2,
        df[lat_col].max() + lat_res / 2,
        lon_res,
        lat_res
    )

    # Create the raster grid
    lon_coords = np.arange(df[lon_col].min(), df[lon_col].max() + lon_res, lon_res)
    lat_coords = np.arange(df[lat_col].min(), df[lat_col].max() + lat_res, lat_res)
    lon_indices = np.searchsorted(lon_coords, df[lon_col].values) -1
    lat_indices = np.searchsorted(lat_coords, df[lat_col].values) -1

    raster = np.full((len(lat_coords), len(lon_coords)), np.nan)
    raster[lat_indices, lon_indices] = df[val_col].values

    return raster, transform

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
    daily_avg_wpd = daily_avg_wpd.rename(columns={'lon': 'x', 'lat': 'y'})

    # Get European country boundaries
    world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
    europe = world[world.continent == 'Europe']
    europe = europe.to_crs(epsg=3857) # Web Mercator for plotting

    # Create faceted plot
    unique_days = sorted(daily_avg_wpd['forecast_day'].unique())
    fig, axes = plt.subplots(1, len(unique_days), figsize=(20, 7), sharey=True)
    if len(unique_days) == 1:
        axes = [axes]

    for i, day in enumerate(unique_days):
        ax = axes[i]
        day_data = daily_avg_wpd[daily_avg_wpd['forecast_day'] == day]
        gdf = gpd.GeoDataFrame(day_data, geometry=gpd.points_from_xy(day_data.x, day_data.y), crs="EPSG:4326")
        gdf = gdf.to_crs(epsg=3857)

        europe.plot(ax=ax, facecolor='none', edgecolor='black', linewidth=0.5)
        gdf.plot(kind="hexbin", x='x', y='y', C='wind_power_density', cmap='viridis', gridsize=40, ax=ax)

        cx.add_basemap(ax, source=cx.providers.CartoDB.Positron)
        ax.set_title(f"Forecast Day: {day.strftime('%Y-%m-%d')}")
        ax.set_xlabel("Longitude")
        if i == 0:
            ax.set_ylabel("Latitude")

    plt.suptitle(f"Daily Average Wind Power Density (GFS Run: {date_str} Cycle {cycle_str})", fontsize=16)
    plt.tight_layout(rect=[0, 0, 1, 0.96])

    # Save the map
    output_dir = Path("data/processed/plots")
    output_dir.mkdir(parents=True, exist_ok=True)
    plot_path = output_dir / f"wpd_map_faceted_{date_str}_{cycle_str}.png"
    plt.savefig(plot_path)
    print(f"Successfully generated and saved faceted wind power density map to {plot_path}")

    # --- 4. Country Ranking ---
    total_avg_wpd = gfs_data.groupby(['lat', 'lon'])['wind_power_density'].mean().reset_index()
    
    # Convert points to GeoDataFrame
    geometry = [Point(xy) for xy in zip(total_avg_wpd.lon, total_avg_wpd.lat)]
    points_gdf = gpd.GeoDataFrame(total_avg_wpd, crs="EPSG:4326", geometry=geometry)

    # Perform spatial join
    countries_gdf = world[['name', 'iso_a3', 'geometry']]
    joined_gdf = gpd.sjoin(points_gdf, countries_gdf, how="inner", op='within')

    # Calculate average WPD per country
    country_avg = joined_gdf.groupby('name')['wind_power_density'].mean().reset_index()
    country_results = country_avg.sort_values('wind_power_density', ascending=False).reset_index(drop=True)
    country_results['rank'] = country_results.index + 1
    country_results = country_results.rename(columns={'wind_power_density': 'avg_wpd_3day'})

    print("\n--- Country Wind Power Density Rankings (3-Day Average) ---")
    print(country_results.head(20))

    # Save rankings to CSV
    csv_path = output_dir / f"country_rankings_{date_str}_{cycle_str}.csv"
    country_results.to_csv(csv_path, index=False)
    print(f"\nSuccessfully saved country rankings to {csv_path}")

    # --- 5. Save to Database ---
    con = sqlite3.connect(db_path)
    cursor = con.cursor()

    # Delete existing rankings
    delete_query = "DELETE FROM country_rankings WHERE forecast_date = ? AND cycle = ?"
    cursor.execute(delete_query, (date_str, cycle_str))
    print(f"\nDeleted existing rankings for {date_str} cycle {cycle_str}.")

    # Insert new rankings
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

import duckdb
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import matplotlib.pyplot as plt
import os
import requests
import zipfile
from pathlib import Path

def get_country_shapes():
    """
    Downloads and returns country shapes for Europe.
    """
    # Define the path for storing the shapefile
    data_dir = Path("data/shapefiles")
    data_dir.mkdir(parents=True, exist_ok=True)
    
    shapefile_path = data_dir / "ne_110m_admin_0_countries.shp"
    
    # Download the shapefile components if they don't exist
    if not shapefile_path.exists():
        print("Downloading Natural Earth countries shapefile components...")
        
        # List of required shapefile components
        base_url = "https://github.com/nvkelso/natural-earth-vector/raw/master/110m_cultural/"
        files_to_download = [
            "ne_110m_admin_0_countries.shp",
            "ne_110m_admin_0_countries.shx", 
            "ne_110m_admin_0_countries.dbf",
            "ne_110m_admin_0_countries.prj",
            "ne_110m_admin_0_countries.cpg"
        ]
        
        for filename in files_to_download:
            file_path = data_dir / filename
            if not file_path.exists():
                print(f"Downloading {filename}...")
                url = base_url + filename
                response = requests.get(url)
                response.raise_for_status()
                
                with open(file_path, 'wb') as f:
                    f.write(response.content)
        
        print("Shapefile components downloaded successfully.")
    
    # Read the shapefile
    world = gpd.read_file(shapefile_path)
    europe = world[world['CONTINENT'] == 'Europe']
    return europe

def create_europe_dashboard_data(db_path, date_str, cycle_str, output_dir):
    """
    Generates data for the Europe dashboard.
    """
    con = duckdb.connect(db_path)
    query = f"""
    SELECT lat, lon, wind_power_density, forecast_hour 
    FROM gfs_forecasts 
    WHERE forecast_date = '{date_str}' AND cycle = '{cycle_str}'
    """
    gfs_data = con.execute(query).fetchdf()
    con.close()

    if gfs_data.empty:
        print("No data found for the specified date and cycle.")
        return

    gfs_data['forecast_datetime'] = pd.to_datetime(date_str) + pd.to_timedelta(gfs_data['forecast_hour'], unit='h')
    gfs_data['forecast_day'] = gfs_data['forecast_datetime'].dt.date

    daily_avg_wpd = gfs_data.groupby(['lat', 'lon', 'forecast_day'])['wind_power_density'].mean().reset_index()

    europe = get_country_shapes()

    fig, ax = plt.subplots(1, 1, figsize=(20, 10))
    europe.boundary.plot(ax=ax)
    
    for day in daily_avg_wpd['forecast_day'].unique():
        day_data = daily_avg_wpd[daily_avg_wpd['forecast_day'] == day]
        plt.scatter(day_data['lon'], day_data['lat'], c=day_data['wind_power_density'], cmap='viridis', s=1)

    plt.title(f"Daily Average Wind Power Density (GFS Run: {date_str} Cycle {cycle_str})")
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    
    plot_path = os.path.join(output_dir, f"wpd_map_faceted_{date_str}_{cycle_str}.png")
    plt.savefig(plot_path)
    plt.close()  # Close the figure to free memory
    print(f"Successfully generated and saved faceted wind power density map to {plot_path}")

    total_avg_wpd = gfs_data.groupby(['lat', 'lon'])['wind_power_density'].mean().reset_index()
    geometry = [Point(xy) for xy in zip(total_avg_wpd['lon'], total_avg_wpd['lat'])]
    points_gdf = gpd.GeoDataFrame(total_avg_wpd, geometry=geometry, crs="EPSG:4326")

    joined_gdf = gpd.sjoin(points_gdf, europe, how="inner", predicate='within')
    # Use the correct column name for country names in Natural Earth data
    country_column = 'NAME' if 'NAME' in joined_gdf.columns else 'name'
    country_avg = joined_gdf.groupby(country_column)['wind_power_density'].mean().reset_index()
    country_avg = country_avg.sort_values(by='wind_power_density', ascending=False).reset_index(drop=True)
    country_avg['rank'] = country_avg.index + 1
    
    csv_path = os.path.join(output_dir, f"country_rankings_{date_str}_{cycle_str}.csv")
    country_avg.to_csv(csv_path, index=False)
    print(f"Successfully saved country rankings to {csv_path}")


def create_specific_points_dashboard_data(db_path, date_str, cycle_str, coordinates_file, output_dir):
    """
    Generates data for the specific points dashboard.
    """
    con = duckdb.connect(db_path)
    query = f"""
    SELECT lat, lon, wind_power_density, forecast_hour 
    FROM gfs_forecasts 
    WHERE forecast_date = '{date_str}' AND cycle = '{cycle_str}'
    """
    gfs_data = con.execute(query).fetchdf()
    con.close()

    if gfs_data.empty:
        print("No data found for the specified date and cycle.")
        return

    coordinates = pd.read_csv(coordinates_file)

    gfs_data['geometry'] = [Point(xy) for xy in zip(gfs_data['lon'], gfs_data['lat'])]
    gfs_gdf = gpd.GeoDataFrame(gfs_data, geometry=gfs_data['geometry'], crs="EPSG:4326")

    coordinates['geometry'] = [Point(xy) for xy in zip(coordinates['lon'], coordinates['lat'])]
    coord_gdf = gpd.GeoDataFrame(coordinates, geometry=coordinates['geometry'], crs="EPSG:4326")

    joined_gdf = gpd.sjoin_nearest(coord_gdf, gfs_gdf, how="inner")
    
    specific_points_avg = joined_gdf.groupby('site_name')['wind_power_density'].mean().reset_index()
    specific_points_avg = specific_points_avg.sort_values(by='wind_power_density', ascending=False).reset_index(drop=True)
    specific_points_avg['rank'] = specific_points_avg.index + 1

    csv_path = os.path.join(output_dir, f"specific_points_rankings_{date_str}_{cycle_str}.csv")
    specific_points_avg.to_csv(csv_path, index=False)
    print(f"Successfully saved specific points rankings to {csv_path}")


def main(date_str, cycle_str):
    db_path = "data/processed/gfs_data.duckdb"
    output_dir = "data/processed/plots"
    coordinates_file = "wind_power_plants_coordinates.csv"
    
    os.makedirs(output_dir, exist_ok=True)

    create_europe_dashboard_data(db_path, date_str, cycle_str, output_dir)
    create_specific_points_dashboard_data(db_path, date_str, cycle_str, coordinates_file, output_dir)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--cycle", required=True)
    args = parser.parse_args()
    main(args.date, args.cycle)

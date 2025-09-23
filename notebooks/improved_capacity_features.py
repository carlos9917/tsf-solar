import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
import warnings
warnings.filterwarnings('ignore')

def create_capacity_features(solar_data: pd.DataFrame) -> pd.DataFrame:
    """
    Create robust capacity trend features that capture structural growth
    in German solar installations.
    """
    print("Creating improved capacity trend features...")

    # Ensure DateTime index
    if 'DateTime' in solar_data.columns:
        solar_data = solar_data.set_index('DateTime')

    data = solar_data.copy()

    # 1. Calculate rolling maximum as capacity proxy
    # Use 90-day rolling max to capture capacity increases
    data['capacity_proxy_90d'] = data['power'].rolling(window=90*24, min_periods=24*30).max()

    # 2. Calculate yearly capacity growth trends
    yearly_stats = data.groupby(data.index.year).agg({
        'power': ['max', lambda x: np.percentile(x, 99), lambda x: np.percentile(x, 95)]
    })
    yearly_stats.columns = ['yearly_max', 'yearly_99p', 'yearly_95p']

    # 3. Fit exponential growth model to yearly maximums
    years = np.array(yearly_stats.index)
    max_values = yearly_stats['yearly_max'].values

    # Remove any years with zero max (shouldn't happen but safety check)
    valid_mask = max_values > 0
    years_valid = years[valid_mask]
    max_values_valid = max_values[valid_mask]

    if len(years_valid) >= 3:
        # Fit exponential model: capacity = a * exp(b * year)
        log_capacity = np.log(max_values_valid)
        poly_features = PolynomialFeatures(degree=1, include_bias=True)
        years_poly = poly_features.fit_transform(years_valid.reshape(-1, 1))

        model = LinearRegression()
        model.fit(years_poly, log_capacity)

        # Extract growth parameters
        growth_rate = model.coef_[1]  # Annual growth rate in log space
        base_capacity = np.exp(model.intercept_)

        print(f"Fitted exponential growth model:")
        print(f"  Annual growth rate: {(np.exp(growth_rate) - 1) * 100:.1f}%")
        print(f"  Base capacity (at year 0): {base_capacity:.0f} MWh")

        # 4. Create time-based features
        # Convert index to numeric (days since start)
        start_date = data.index.min()
        data['days_since_start'] = (data.index - start_date).days
        data['years_since_start'] = data['days_since_start'] / 365.25

        # Actual year (for discrete capacity jumps)
        data['year'] = data.index.year

        # 5. Exponential capacity trend feature
        reference_year = 2022  # Use as baseline
        data['exp_capacity_trend'] = base_capacity * np.exp(growth_rate * (data['year'] - reference_year))

        # 6. Linear capacity trend (alternative)
        data['linear_capacity_trend'] = data['days_since_start'] * (max_values_valid[-1] - max_values_valid[0]) / (years_valid[-1] - years_valid[0]) / 365.25

        # 7. Capacity growth rate features
        data['capacity_growth_rate'] = growth_rate

        # 8. Year dummy variables (for discrete capacity additions)
        for year in range(2022, 2026):
            data[f'year_{year}'] = (data['year'] == year).astype(int)

        # 9. Seasonal capacity scaling
        # Assume new capacity is added more in certain months
        month_capacity_factors = {
            1: 0.8, 2: 0.9, 3: 1.0, 4: 1.1, 5: 1.2, 6: 1.2,
            7: 1.1, 8: 1.0, 9: 1.1, 10: 1.0, 11: 0.9, 12: 0.8
        }
        data['seasonal_capacity_factor'] = data.index.month.map(month_capacity_factors)
        data['adjusted_capacity_trend'] = data['exp_capacity_trend'] * data['seasonal_capacity_factor']

        # 10. Recent trend (last 2 years get higher weight)
        recent_cutoff = pd.Timestamp('2023-01-01')
        # Make timezone-aware if data index is timezone-aware
        if data.index.tz is not None:
            recent_cutoff = recent_cutoff.tz_localize(data.index.tz)
        data['is_recent'] = (data.index >= recent_cutoff).astype(int)

        # 11. Capacity utilization features
        data['capacity_utilization'] = data['power'] / (data['capacity_proxy_90d'] + 1e-6)
        data['capacity_utilization'] = np.clip(data['capacity_utilization'], 0, 1)

    else:
        print("Warning: Insufficient data for capacity trend modeling")
        growth_rate = 0.1  # Default 10% annual growth

    return data, growth_rate

def estimate_2025_capacity_adjustment(solar_data: pd.DataFrame, growth_rate: float) -> float:
    """
    Estimate the capacity adjustment factor for 2025 predictions.
    """
    # Get 2024 peak capacity
    data_2024 = solar_data[solar_data.index.year == 2024]
    if len(data_2024) == 0:
        print("Warning: No 2024 data available for capacity estimation")
        return 1.2  # Default 20% increase

    capacity_2024 = data_2024['power'].max()

    # Estimate 2025 capacity using fitted growth rate
    capacity_2025_estimated = capacity_2024 * np.exp(growth_rate)

    adjustment_factor = capacity_2025_estimated / capacity_2024

    print(f"2024 peak capacity: {capacity_2024:.0f} MWh")
    print(f"Estimated 2025 peak capacity: {capacity_2025_estimated:.0f} MWh")
    print(f"Capacity adjustment factor: {adjustment_factor:.3f}")

    return adjustment_factor

def create_weighted_training_data(X_train: pd.DataFrame, y_train: pd.Series,
                                decay_factor: float = 365) -> np.ndarray:
    """
    Create exponential weights that emphasize recent data for training.
    """
    # Calculate days from the end of training period
    end_date = X_train.index.max()
    days_from_end = (end_date - X_train.index).days.values

    # Exponential decay weights (more recent = higher weight)
    weights = np.exp(-days_from_end / decay_factor)

    # Normalize weights
    weights = weights / weights.mean()

    print(f"Training weights - Min: {weights.min():.3f}, Max: {weights.max():.3f}")
    print(f"Recent data (last 6 months) average weight: {weights[days_from_end <= 180].mean():.3f}")
    print(f"Old data (first 6 months) average weight: {weights[days_from_end >= (days_from_end.max() - 180)].mean():.3f}")

    return weights

if __name__ == "__main__":
    # Load and test the features
    print("Loading solar generation data...")
    solar_data = pd.read_csv('/home/tenantadmin/tsf-solar/data/germany_solar_observation_q1.csv')
    solar_data['DateTime'] = pd.to_datetime(solar_data['DateTime'])
    solar_data = solar_data.set_index('DateTime')

    # Create features
    enhanced_data, growth_rate = create_capacity_features(solar_data)

    # Estimate 2025 adjustment
    adjustment_factor = estimate_2025_capacity_adjustment(enhanced_data, growth_rate)

    # Save enhanced dataset
    enhanced_data.to_csv('/home/tenantadmin/tsf-solar/data/solar_data_with_capacity_features.csv')

    print(f"\nEnhanced dataset saved with {len(enhanced_data.columns)} features")
    print("New capacity-related features:")
    capacity_cols = [col for col in enhanced_data.columns if 'capacity' in col.lower() or 'trend' in col.lower() or 'year_' in col]
    for col in capacity_cols:
        print(f"  - {col}")

    # Visualization
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))

    # Plot 1: Capacity trend vs actual max power
    monthly_max = enhanced_data.groupby([enhanced_data.index.year, enhanced_data.index.month]).agg({
        'power': 'max',
        'exp_capacity_trend': 'mean'
    })
    monthly_max.index = pd.to_datetime([f'{year}-{month:02d}-01' for year, month in monthly_max.index])

    axes[0, 0].plot(monthly_max.index, monthly_max['power'], 'o-', label='Actual Max Power', alpha=0.7)
    axes[0, 0].plot(monthly_max.index, monthly_max['exp_capacity_trend'], 's-', label='Exponential Trend', alpha=0.7)
    axes[0, 0].set_title('Capacity Trend vs Actual Maximum Power')
    axes[0, 0].set_ylabel('Power (MWh)')
    axes[0, 0].legend()
    axes[0, 0].grid(True, alpha=0.3)

    # Plot 2: Capacity utilization over time
    monthly_util = enhanced_data.groupby([enhanced_data.index.year, enhanced_data.index.month])['capacity_utilization'].mean()
    monthly_util.index = pd.to_datetime([f'{year}-{month:02d}-01' for year, month in monthly_util.index])

    axes[0, 1].plot(monthly_util.index, monthly_util.values, 'g-', linewidth=2)
    axes[0, 1].set_title('Average Capacity Utilization Over Time')
    axes[0, 1].set_ylabel('Capacity Utilization')
    axes[0, 1].grid(True, alpha=0.3)

    # Plot 3: Recent data (2024-2025) with trend
    recent_data = enhanced_data[enhanced_data.index >= '2024-01-01']
    daily_max = recent_data.groupby(recent_data.index.date).agg({
        'power': 'max',
        'exp_capacity_trend': 'mean'
    })

    axes[1, 0].plot(daily_max.index, daily_max['power'], alpha=0.6, label='Daily Max Power')
    axes[1, 0].plot(daily_max.index, daily_max['exp_capacity_trend'], 'r-', label='Capacity Trend', linewidth=2)
    axes[1, 0].set_title('2024-2025: Actual vs Trend')
    axes[1, 0].set_ylabel('Power (MWh)')
    axes[1, 0].legend()
    axes[1, 0].grid(True, alpha=0.3)

    # Plot 4: Sample weights for recent vs old data
    sample_weights = create_weighted_training_data(
        enhanced_data[enhanced_data.index < '2025-01-01'],
        enhanced_data[enhanced_data.index < '2025-01-01']['power']
    )

    weight_df = pd.DataFrame({
        'date': enhanced_data[enhanced_data.index < '2025-01-01'].index,
        'weight': sample_weights
    })
    monthly_weights = weight_df.groupby([weight_df['date'].dt.year, weight_df['date'].dt.month])['weight'].mean()
    monthly_weights.index = pd.to_datetime([f'{year}-{month:02d}-01' for year, month in monthly_weights.index])

    axes[1, 1].plot(monthly_weights.index, monthly_weights.values, 'purple', linewidth=2)
    axes[1, 1].set_title('Training Data Weights (Recent Emphasis)')
    axes[1, 1].set_ylabel('Weight')
    axes[1, 1].grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig('/home/tenantadmin/tsf-solar/improved_capacity_analysis.png', dpi=150, bbox_inches='tight')
    plt.show()
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from typing import Union, Tuple
import warnings
warnings.filterwarnings('ignore')

class CapacityAdjustmentProcessor:
    """
    Post-processing tool to adjust existing forecasts for capacity growth trends.
    This can be applied to your original forecast to correct for the structural bias.
    """

    def __init__(self):
        self.adjustment_factor = None
        self.hourly_adjustment_factors = None
        self.seasonal_adjustment_factors = None

    def calculate_adjustment_factors(self, solar_data: pd.DataFrame) -> dict:
        """
        Calculate various adjustment factors based on historical capacity trends.
        """
        print("Calculating capacity adjustment factors...")

        # Ensure DateTime index
        if 'DateTime' in solar_data.columns:
            solar_data = solar_data.set_index('DateTime')

        # 1. Overall capacity growth factor
        yearly_peaks = solar_data.groupby(solar_data.index.year)['power'].max()

        if len(yearly_peaks) >= 3:
            # Calculate compound annual growth rate (CAGR)
            years = len(yearly_peaks) - 1
            cagr = (yearly_peaks.iloc[-1] / yearly_peaks.iloc[0]) ** (1/years) - 1

            # Project to 2025
            last_year = yearly_peaks.index[-1]
            years_to_2025 = 2025 - last_year
            overall_factor = (1 + cagr) ** years_to_2025

            print(f"Historical CAGR: {cagr*100:.1f}%")
            print(f"Overall capacity adjustment factor for 2025: {overall_factor:.3f}")

        else:
            overall_factor = 1.2  # Default 20% increase
            print("Insufficient data, using default 20% capacity increase")

        # 2. Hourly adjustment factors (some hours see more capacity benefit)
        # Peak solar hours typically benefit more from new capacity
        hourly_factors = {}
        for hour in range(24):
            if 8 <= hour <= 17:  # Daylight hours
                # Peak hours (11-15) get highest benefit
                if 11 <= hour <= 15:
                    factor = overall_factor * 1.1  # 10% bonus for peak hours
                else:
                    factor = overall_factor
            else:
                factor = 1.0  # No adjustment for night hours

            hourly_factors[hour] = factor

        # 3. Seasonal adjustment factors
        seasonal_factors = {
            1: 0.95, 2: 0.97, 3: 1.0, 4: 1.05, 5: 1.08, 6: 1.1,
            7: 1.08, 8: 1.05, 9: 1.02, 10: 1.0, 11: 0.97, 12: 0.95
        }

        # Store factors
        self.adjustment_factor = overall_factor
        self.hourly_adjustment_factors = hourly_factors
        self.seasonal_adjustment_factors = seasonal_factors

        return {
            'overall_factor': overall_factor,
            'hourly_factors': hourly_factors,
            'seasonal_factors': seasonal_factors,
            'cagr': cagr if 'cagr' in locals() else 0.1
        }

    def apply_smart_adjustment(self, forecast_df: pd.DataFrame,
                             solar_data: pd.DataFrame = None,
                             method: str = 'comprehensive') -> pd.DataFrame:
        """
        Apply intelligent capacity adjustment to existing forecasts.

        Parameters:
        - forecast_df: DataFrame with DateTime and power columns
        - solar_data: Historical solar data for calculating adjustments
        - method: 'simple', 'hourly', or 'comprehensive'
        """
        forecast_adjusted = forecast_df.copy()

        # Ensure DateTime is datetime type
        if 'DateTime' in forecast_adjusted.columns:
            forecast_adjusted['DateTime'] = pd.to_datetime(forecast_adjusted['DateTime'])

        # Calculate adjustment factors if not already done
        if self.adjustment_factor is None and solar_data is not None:
            self.calculate_adjustment_factors(solar_data)
        elif self.adjustment_factor is None:
            print("Warning: No adjustment factors calculated. Using default values.")
            self.adjustment_factor = 1.2
            self.hourly_adjustment_factors = {hour: 1.2 if 8 <= hour <= 17 else 1.0
                                            for hour in range(24)}
            self.seasonal_adjustment_factors = {month: 1.0 for month in range(1, 13)}

        if method == 'simple':
            # Simple multiplicative adjustment
            forecast_adjusted['power'] = forecast_adjusted['power'] * self.adjustment_factor

        elif method == 'hourly':
            # Hour-aware adjustment
            hours = pd.to_datetime(forecast_adjusted['DateTime']).dt.hour
            adjustments = [self.hourly_adjustment_factors[hour] for hour in hours]
            forecast_adjusted['power'] = forecast_adjusted['power'] * adjustments

        elif method == 'comprehensive':
            # Full comprehensive adjustment
            dates = pd.to_datetime(forecast_adjusted['DateTime'])
            hours = dates.dt.hour
            months = dates.dt.month

            # Combine hourly and seasonal factors
            total_adjustments = []
            for hour, month in zip(hours, months):
                base_factor = self.hourly_adjustment_factors[hour]
                seasonal_factor = self.seasonal_adjustment_factors[month]
                total_factor = base_factor * seasonal_factor
                total_adjustments.append(total_factor)

            forecast_adjusted['power'] = forecast_adjusted['power'] * total_adjustments
            forecast_adjusted['adjustment_factor'] = total_adjustments

        # Ensure non-negative values
        forecast_adjusted['power'] = np.maximum(0, forecast_adjusted['power'])

        print(f"Applied {method} capacity adjustment")
        print(f"Original mean power: {forecast_df['power'].mean():.2f} MWh")
        print(f"Adjusted mean power: {forecast_adjusted['power'].mean():.2f} MWh")
        print(f"Mean adjustment factor: {forecast_adjusted['power'].mean() / forecast_df['power'].mean():.3f}")

        return forecast_adjusted

    def apply_probabilistic_adjustment(self, prob_forecast_df: pd.DataFrame,
                                     solar_data: pd.DataFrame = None) -> pd.DataFrame:
        """
        Apply capacity adjustment to probabilistic forecasts (multiple quantiles).
        """
        prob_adjusted = prob_forecast_df.copy()

        # Calculate adjustment factors
        if self.adjustment_factor is None and solar_data is not None:
            self.calculate_adjustment_factors(solar_data)

        # Find power columns (quantiles)
        power_cols = [col for col in prob_adjusted.columns if col.startswith('power')]

        if len(power_cols) == 0:
            print("No power columns found for probabilistic adjustment")
            return prob_adjusted

        # Apply comprehensive adjustment to all quantiles
        dates = pd.to_datetime(prob_adjusted['DateTime'])
        hours = dates.dt.hour
        months = dates.dt.month

        # Calculate adjustment factors
        total_adjustments = []
        for hour, month in zip(hours, months):
            base_factor = self.hourly_adjustment_factors.get(hour, 1.0)
            seasonal_factor = self.seasonal_adjustment_factors.get(month, 1.0)
            total_factor = base_factor * seasonal_factor
            total_adjustments.append(total_factor)

        # Apply to all power columns
        for col in power_cols:
            prob_adjusted[col] = prob_adjusted[col] * total_adjustments

        # Ensure non-negative values
        for col in power_cols:
            prob_adjusted[col] = np.maximum(0, prob_adjusted[col])

        print(f"Applied probabilistic capacity adjustment to {len(power_cols)} quantiles")

        return prob_adjusted

    def visualize_adjustment_impact(self, original_forecast: pd.DataFrame,
                                  adjusted_forecast: pd.DataFrame,
                                  save_path: str = None) -> None:
        """
        Visualize the impact of capacity adjustments.
        """
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))

        dates = pd.to_datetime(original_forecast['DateTime'])

        # Plot 1: Original vs Adjusted time series (first week)
        week_mask = dates < dates.min() + pd.Timedelta(days=7)
        axes[0, 0].plot(dates[week_mask], original_forecast.loc[week_mask, 'power'],
                       'b-', label='Original', alpha=0.7)
        axes[0, 0].plot(dates[week_mask], adjusted_forecast.loc[week_mask, 'power'],
                       'r-', label='Adjusted', alpha=0.7)
        axes[0, 0].set_title('First Week: Original vs Adjusted Forecast')
        axes[0, 0].set_ylabel('Power (MWh)')
        axes[0, 0].legend()
        axes[0, 0].grid(True, alpha=0.3)

        # Plot 2: Daily totals comparison
        daily_orig = original_forecast.set_index(dates).resample('D')['power'].sum()
        daily_adj = adjusted_forecast.set_index(dates).resample('D')['power'].sum()

        axes[0, 1].plot(daily_orig.index, daily_orig.values, 'b-', label='Original', linewidth=2)
        axes[0, 1].plot(daily_adj.index, daily_adj.values, 'r-', label='Adjusted', linewidth=2)
        axes[0, 1].set_title('Daily Total Generation')
        axes[0, 1].set_ylabel('Daily Power (MWh)')
        axes[0, 1].legend()
        axes[0, 1].grid(True, alpha=0.3)

        # Plot 3: Hourly adjustment factors
        if hasattr(self, 'hourly_adjustment_factors') and self.hourly_adjustment_factors:
            hours = list(self.hourly_adjustment_factors.keys())
            factors = list(self.hourly_adjustment_factors.values())
            axes[1, 0].bar(hours, factors, alpha=0.7, color='green')
            axes[1, 0].axhline(y=1.0, color='black', linestyle='--', alpha=0.5)
            axes[1, 0].set_title('Hourly Adjustment Factors')
            axes[1, 0].set_xlabel('Hour of Day')
            axes[1, 0].set_ylabel('Adjustment Factor')
            axes[1, 0].grid(True, alpha=0.3)

        # Plot 4: Distribution comparison
        axes[1, 1].hist(original_forecast['power'], bins=50, alpha=0.6, label='Original', density=True)
        axes[1, 1].hist(adjusted_forecast['power'], bins=50, alpha=0.6, label='Adjusted', density=True)
        axes[1, 1].set_title('Power Distribution Comparison')
        axes[1, 1].set_xlabel('Power (MWh)')
        axes[1, 1].set_ylabel('Density')
        axes[1, 1].legend()
        axes[1, 1].grid(True, alpha=0.3)

        plt.tight_layout()

        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
            print(f"Visualization saved to {save_path}")

        plt.show()

def quick_capacity_fix(forecast_csv_path: str, solar_data_csv_path: str,
                      output_path: str = None, method: str = 'comprehensive') -> str:
    """
    Quick function to apply capacity adjustment to an existing forecast CSV.
    """
    print(f"Applying capacity adjustment to {forecast_csv_path}")

    # Load data
    forecast_df = pd.read_csv(forecast_csv_path)
    solar_data = pd.read_csv(solar_data_csv_path)
    solar_data['DateTime'] = pd.to_datetime(solar_data['DateTime'])

    # Apply adjustment
    processor = CapacityAdjustmentProcessor()
    adjusted_forecast = processor.apply_smart_adjustment(forecast_df, solar_data, method=method)

    # Save result
    if output_path is None:
        output_path = forecast_csv_path.replace('.csv', '_capacity_adjusted.csv')

    adjusted_forecast[['DateTime', 'power']].to_csv(output_path, index=False)

    print(f"Capacity-adjusted forecast saved to {output_path}")

    # Show impact summary
    original_total = forecast_df['power'].sum()
    adjusted_total = adjusted_forecast['power'].sum()
    increase_pct = (adjusted_total - original_total) / original_total * 100

    print(f"\nAdjustment Impact:")
    print(f"Original total: {original_total:,.0f} MWh")
    print(f"Adjusted total: {adjusted_total:,.0f} MWh")
    print(f"Increase: {increase_pct:.1f}%")

    return output_path

if __name__ == "__main__":
    # Example: Fix your original forecast
    print("=== Capacity Adjustment Post-Processing ===")

    # Check if original forecast exists
    import os
    forecast_path = '/home/tenantadmin/tsf-solar/forecast_q1.csv'
    solar_data_path = '/home/tenantadmin/tsf-solar/data/germany_solar_observation_q1.csv'

    if os.path.exists(forecast_path):
        print("Found original forecast, applying capacity adjustment...")

        adjusted_path = quick_capacity_fix(
            forecast_path,
            solar_data_path,
            output_path='/home/tenantadmin/tsf-solar/forecast_q1_capacity_adjusted.csv',
            method='comprehensive'
        )

        print(f"✅ Capacity-adjusted forecast saved to: {adjusted_path}")

        # Also check for probabilistic forecast
        prob_forecast_path = '/home/tenantadmin/tsf-solar/forecast_q1_probabilistic.csv'
        if os.path.exists(prob_forecast_path):
            print("\nAdjusting probabilistic forecast...")

            prob_df = pd.read_csv(prob_forecast_path)
            solar_data = pd.read_csv(solar_data_path)
            solar_data['DateTime'] = pd.to_datetime(solar_data['DateTime'])

            processor = CapacityAdjustmentProcessor()
            adjusted_prob = processor.apply_probabilistic_adjustment(prob_df, solar_data)

            prob_adjusted_path = '/home/tenantadmin/tsf-solar/forecast_q1_probabilistic_capacity_adjusted.csv'
            adjusted_prob.to_csv(prob_adjusted_path, index=False)

            print(f"✅ Probabilistic forecast adjusted and saved to: {prob_adjusted_path}")

    else:
        print(f"Original forecast not found at {forecast_path}")
        print("Run the main model first, then use this script to adjust for capacity trends.")
#!/usr/bin/env python3
"""
Analyze seasonal capacity patterns from available data to replace hardcoded values
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

def analyze_capacity_seasonality(solar_data_path):
    """
    Extract seasonal capacity installation patterns from historical data
    """
    print("Loading solar observation data...")
    df = pd.read_csv(solar_data_path, parse_dates=['DateTime'])
    df = df.set_index('DateTime').sort_index()

    print(f"Data range: {df.index.min()} to {df.index.max()}")
    print(f"Total observations: {len(df)}")

    # 1. Calculate rolling maximum peaks to estimate capacity
    print("\n1. Analyzing capacity trends from rolling peaks...")

    # Use 30-day rolling maximum to smooth out weather variations
    df['rolling_max_30d'] = df['power'].rolling(window=30*24, min_periods=24*7).max()

    # Monthly maximum peaks (proxy for installed capacity)
    monthly_peaks = df.groupby([df.index.year, df.index.month])['power'].max()
    monthly_peaks_df = monthly_peaks.unstack(level=0)

    print("Monthly peak power by year:")
    print(monthly_peaks_df)

    # 2. Analyze month-over-month capacity changes
    print("\n2. Analyzing month-over-month capacity changes...")

    # Calculate capacity additions (positive changes in peak power)
    capacity_changes = {}
    total_additions_by_month = {month: [] for month in range(1, 13)}

    for year in monthly_peaks_df.columns:
        year_data = monthly_peaks_df[year].dropna()
        for i in range(1, len(year_data)):
            month = year_data.index[i]
            prev_month = year_data.index[i-1]
            change = year_data.iloc[i] - year_data.iloc[i-1]

            # Only count positive changes as capacity additions
            if change > 0:
                total_additions_by_month[month].append(change)

    # Calculate average additions by month
    avg_additions_by_month = {}
    for month in range(1, 13):
        if total_additions_by_month[month]:
            avg_additions_by_month[month] = np.mean(total_additions_by_month[month])
        else:
            avg_additions_by_month[month] = 0

    print("Average capacity additions by month (MWh):")
    for month, addition in avg_additions_by_month.items():
        print(f"  Month {month:2d}: {addition:6.1f} MWh")

    # 3. Calculate seasonal factors (data-driven)
    print("\n3. Calculating data-driven seasonal factors...")

    total_additions = sum(avg_additions_by_month.values())
    if total_additions > 0:
        seasonal_factors = {
            month: (addition / (total_additions / 12)) if total_additions > 0 else 1.0
            for month, addition in avg_additions_by_month.items()
        }
    else:
        # Conservative fallback: no seasonal bias
        seasonal_factors = {month: 1.0 for month in range(1, 13)}

    print("Data-driven seasonal capacity factors:")
    for month, factor in seasonal_factors.items():
        print(f"  Month {month:2d}: {factor:.3f}")

    # 4. Compare with hardcoded values
    hardcoded_factors = {
        1: 0.8, 2: 0.9, 3: 1.0, 4: 1.1, 5: 1.2, 6: 1.2,
        7: 1.1, 8: 1.0, 9: 1.1, 10: 1.0, 11: 0.9, 12: 0.8
    }

    print("\n4. Comparison with hardcoded values:")
    print("Month | Data-Driven | Hardcoded | Difference")
    print("------|-------------|-----------|----------")
    for month in range(1, 13):
        data_val = seasonal_factors[month]
        hard_val = hardcoded_factors[month]
        diff = data_val - hard_val
        print(f"  {month:2d}  |    {data_val:.3f}    |   {hard_val:.3f}   |   {diff:+.3f}")

    # 5. Analyze year-over-year growth patterns
    print("\n5. Year-over-year growth analysis...")

    yearly_peaks = df.groupby(df.index.year)['power'].max()
    print("Annual peak capacity:")
    growth_rates = []
    for i, (year, peak) in enumerate(yearly_peaks.items()):
        if i > 0:
            prev_peak = yearly_peaks.iloc[i-1]
            growth_rate = (peak - prev_peak) / prev_peak * 100
            growth_rates.append(growth_rate)
            print(f"  {year}: {peak:6.0f} MWh (+{growth_rate:5.1f}%)")
        else:
            print(f"  {year}: {peak:6.0f} MWh")

    avg_growth = np.mean(growth_rates) if growth_rates else 0
    print(f"Average annual growth rate: {avg_growth:.1f}%")

    # 6. Visualization
    print("\n6. Creating visualizations...")

    fig, axes = plt.subplots(2, 2, figsize=(15, 10))

    # Plot 1: Monthly peaks over time
    axes[0, 0].plot(monthly_peaks.index.get_level_values(0) +
                    monthly_peaks.index.get_level_values(1)/12,
                    monthly_peaks.values, 'o-', alpha=0.7)
    axes[0, 0].set_title('Monthly Peak Power Over Time')
    axes[0, 0].set_xlabel('Year')
    axes[0, 0].set_ylabel('Peak Power (MWh)')
    axes[0, 0].grid(True, alpha=0.3)

    # Plot 2: Seasonal factors comparison
    months = list(range(1, 13))
    data_factors = [seasonal_factors[m] for m in months]
    hard_factors = [hardcoded_factors[m] for m in months]

    x = np.arange(12)
    width = 0.35
    axes[0, 1].bar(x - width/2, data_factors, width, label='Data-Driven', alpha=0.7)
    axes[0, 1].bar(x + width/2, hard_factors, width, label='Hardcoded', alpha=0.7)
    axes[0, 1].set_title('Seasonal Factors Comparison')
    axes[0, 1].set_xlabel('Month')
    axes[0, 1].set_ylabel('Seasonal Factor')
    axes[0, 1].set_xticks(x)
    axes[0, 1].set_xticklabels(months)
    axes[0, 1].legend()
    axes[0, 1].grid(True, alpha=0.3)

    # Plot 3: Capacity additions by month
    months_with_data = [m for m in months if avg_additions_by_month[m] > 0]
    additions = [avg_additions_by_month[m] for m in months_with_data]

    if additions:
        axes[1, 0].bar(months_with_data, additions, alpha=0.7, color='green')
        axes[1, 0].set_title('Average Capacity Additions by Month')
        axes[1, 0].set_xlabel('Month')
        axes[1, 0].set_ylabel('Avg Addition (MWh)')
        axes[1, 0].grid(True, alpha=0.3)
    else:
        axes[1, 0].text(0.5, 0.5, 'No clear capacity\naddition pattern\ndetected',
                        ha='center', va='center', transform=axes[1, 0].transAxes)
        axes[1, 0].set_title('Capacity Additions by Month')

    # Plot 4: Year-over-year growth
    if len(yearly_peaks) > 1:
        years = yearly_peaks.index[1:]
        axes[1, 1].plot(years, growth_rates, 'o-', linewidth=2, markersize=8)
        axes[1, 1].axhline(y=avg_growth, color='red', linestyle='--',
                          label=f'Average: {avg_growth:.1f}%')
        axes[1, 1].set_title('Annual Capacity Growth Rate')
        axes[1, 1].set_xlabel('Year')
        axes[1, 1].set_ylabel('Growth Rate (%)')
        axes[1, 1].legend()
        axes[1, 1].grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig('/home/tenantadmin/tsf-solar/notebooks/capacity_seasonality_analysis.png',
                dpi=150, bbox_inches='tight')
    plt.show()

    return seasonal_factors, avg_additions_by_month, yearly_peaks

if __name__ == "__main__":
    seasonal_factors, additions, yearly_peaks = analyze_capacity_seasonality(
        "/home/tenantadmin/tsf-solar/data/germany_solar_observation_q1.csv"
    )

    print(f"\n✅ Analysis complete! Results saved to capacity_seasonality_analysis.png")
    print(f"Use these data-driven seasonal factors instead of hardcoded values:")
    print(f"seasonal_factors = {seasonal_factors}")
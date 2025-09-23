import pandas as pd
import numpy as np
import lightgbm as lgb
from typing import Dict, Tuple, List, Optional
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import warnings
warnings.filterwarnings('ignore')

class ImprovedQuantileLightGBM:
    """
    Enhanced LightGBM model that addresses capacity growth trends through:
    1. Weighted training (emphasizing recent data)
    2. Capacity-aware features
    3. Post-processing adjustments
    """

    def __init__(self, quantiles: List[float] = [0.1, 0.25, 0.5, 0.75, 0.9]):
        self.quantiles = quantiles
        self.models = {}
        self.capacity_adjustment_factor = 1.0
        self.training_weights = None

        self.base_params = {
            'boosting_type': 'gbdt',
            'num_leaves': 64,
            'learning_rate': 0.02,  # Slightly lower for stability
            'feature_fraction': 0.85,
            'bagging_fraction': 0.85,
            'bagging_freq': 1,
            'verbose': -1,
            'n_jobs': -1,
            'random_state': 42,
            'n_estimators': 3000,  # More trees for complex patterns
            'early_stopping_rounds': 150,
            'min_child_samples': 20,
            'reg_alpha': 0.1,  # L1 regularization
            'reg_lambda': 0.1   # L2 regularization
        }

    def fit(self, X_train, y_train, X_val=None, y_val=None,
            sample_weights=None, capacity_adjustment=1.0):
        """
        Train models with optional sample weights and capacity awareness.
        """
        self.capacity_adjustment_factor = capacity_adjustment
        self.training_weights = sample_weights

        print(f"Training with capacity adjustment factor: {capacity_adjustment:.3f}")

        for q in self.quantiles:
            print(f"Training quantile {q:.2f}...", end=" ")

            params = self.base_params.copy()
            params['objective'] = 'quantile'
            params['alpha'] = q
            params['metric'] = 'quantile'

            model = lgb.LGBMRegressor(**params)

            # Prepare training arguments
            fit_params = {}
            if sample_weights is not None:
                fit_params['sample_weight'] = sample_weights

            if X_val is not None and y_val is not None:
                fit_params['eval_set'] = [(X_val, y_val)]
                fit_params['callbacks'] = [lgb.early_stopping(params['early_stopping_rounds'], verbose=False)]

            # Train the model
            model.fit(X_train, y_train, **fit_params)

            self.models[q] = model
            print(f"Done (best iteration: {getattr(model, 'best_iteration_', 'N/A')})")

        return self

    def predict(self, X, apply_capacity_adjustment=True) -> Dict[str, np.ndarray]:
        """
        Generate predictions with optional capacity adjustment.
        """
        predictions = {}

        for q in self.quantiles:
            pred = self.models[q].predict(X)

            # Apply capacity adjustment if specified
            if apply_capacity_adjustment and self.capacity_adjustment_factor != 1.0:
                pred = pred * self.capacity_adjustment_factor

            predictions[f'q{int(q*100)}'] = np.maximum(0, pred)

        return predictions

    def predict_with_confidence(self, X, confidence_level=0.8) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        Generate point prediction with confidence intervals.
        """
        predictions = self.predict(X)

        alpha = 1 - confidence_level
        lower_q = int((alpha / 2) * 100)
        upper_q = int((1 - alpha / 2) * 100)

        # Find closest available quantiles
        available_q = [int(q * 100) for q in self.quantiles]
        lower_key = f'q{min(available_q, key=lambda x: abs(x - lower_q))}'
        upper_key = f'q{min(available_q, key=lambda x: abs(x - upper_q))}'
        median_key = 'q50'

        return predictions[median_key], predictions[lower_key], predictions[upper_key]


class CapacityAwareForecastingPipeline:
    """
    Complete forecasting pipeline that handles capacity growth trends.
    """

    def __init__(self, quantiles: List[float] = [0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95]):
        self.quantiles = quantiles
        self.model = None
        self.capacity_growth_rate = None
        self.feature_importance = None

    def prepare_features(self, solar_data: pd.DataFrame, atm_data: pd.DataFrame) -> pd.DataFrame:
        """
        Merge and prepare features including capacity trends.
        """
        from improved_capacity_features import create_capacity_features

        # Merge datasets
        data = pd.merge(solar_data, atm_data, on="DateTime", how="inner")
        data = data.set_index('DateTime').sort_index()

        # Add capacity features
        data, growth_rate = create_capacity_features(data)
        self.capacity_growth_rate = growth_rate

        # Add original solar and weather features
        data = self._add_solar_weather_features(data)

        return data

    def _add_solar_weather_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add the original high-quality solar and weather features.
        """
        # Basic temporal features
        df['hour'] = df.index.hour
        df['day_of_year'] = df.index.dayofyear
        df['month'] = df.index.month
        df['is_weekend'] = df.index.dayofweek.isin([5, 6]).astype(int)

        # Solar position calculations (from original code)
        latitude = 51.5  # Germany
        df['declination'] = 23.45 * np.sin(np.radians(360 * (284 + df['day_of_year']) / 365))
        df['hour_angle'] = 15 * (df['hour'] - 12)

        elevation_rad = np.arcsin(
            np.sin(np.radians(df['declination'])) * np.sin(np.radians(latitude)) +
            np.cos(np.radians(df['declination'])) * np.cos(np.radians(latitude)) *
            np.cos(np.radians(df['hour_angle']))
        )

        df['solar_elevation'] = np.maximum(0, np.degrees(elevation_rad))
        df['solar_zenith'] = 90 - df['solar_elevation']

        # Air mass calculation
        df['air_mass'] = np.where(
            df['solar_elevation'] > 0,
            1 / (np.sin(np.radians(df['solar_elevation'])) +
                 0.50572 * (df['solar_elevation'] + 6.07995) ** -1.6364),
            0
        )
        df['air_mass'] = np.minimum(df['air_mass'], 40)

        # Cyclical encoding
        df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
        df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)
        df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
        df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)

        # Weather interactions
        df['clear_sky_index'] = df['surface_solar_radiation_downwards'] * (1 - df['total_cloud_cover'] / 100)
        df['cloud_impact'] = df['total_cloud_cover'] * df['solar_elevation']
        df['temp_efficiency'] = np.exp(-((df['temperature_2m'] - 25) / 20) ** 2)
        df['wind_cooling'] = np.log1p(df['wind_speed_100m']) * df['temperature_2m']
        df['is_daylight'] = (df['solar_elevation'] > 0).astype(int)
        df['theoretical_max'] = df['solar_elevation'] * 1000 / 90

        return df

    def create_train_val_split(self, data: pd.DataFrame, val_start='2025-01-01'):
        """
        Create time-aware train/validation split with enhanced features.
        """
        # Define core features (excluding target and metadata)
        feature_cols = [
            # Weather features
            'surface_solar_radiation_downwards', 'temperature_2m', 'total_cloud_cover',
            'relative_humidity_2m', 'apparent_temperature', 'wind_speed_100m',
            # Solar position features
            'hour_sin', 'hour_cos', 'month_sin', 'month_cos',
            'solar_elevation', 'solar_zenith', 'air_mass',
            'clear_sky_index', 'cloud_impact', 'temp_efficiency', 'wind_cooling',
            'is_daylight', 'theoretical_max',
            # Capacity features
            'exp_capacity_trend', 'linear_capacity_trend', 'capacity_growth_rate',
            'seasonal_capacity_factor', 'adjusted_capacity_trend', 'is_recent',
            'capacity_utilization', 'days_since_start', 'years_since_start'
        ]

        # Add year dummies
        year_cols = [col for col in data.columns if col.startswith('year_')]
        feature_cols.extend(year_cols)

        # Filter to available features
        feature_cols = [col for col in feature_cols if col in data.columns]

        # Split data
        train_data = data[data.index < val_start].copy()
        val_data = data[data.index >= val_start].copy()

        # Remove June 2025 if present (that's our forecast target)
        val_data = val_data[val_data.index < '2025-06-01']

        return {
            'X_train': train_data[feature_cols],
            'y_train': train_data['power'],
            'X_val': val_data[feature_cols],
            'y_val': val_data['power'],
            'feature_cols': feature_cols,
            'train_data': train_data,
            'val_data': val_data
        }

    def fit(self, data: pd.DataFrame, val_start='2025-01-01', weight_decay=365):
        """
        Fit the complete pipeline with capacity-aware training.
        """
        from improved_capacity_features import create_weighted_training_data, estimate_2025_capacity_adjustment

        # Create train/val split
        splits = self.create_train_val_split(data, val_start)

        # Create training weights (emphasize recent data)
        training_weights = create_weighted_training_data(
            splits['X_train'], splits['y_train'], decay_factor=weight_decay
        )

        # Estimate capacity adjustment for 2025
        capacity_adjustment = estimate_2025_capacity_adjustment(data, self.capacity_growth_rate)

        # Initialize and train model
        self.model = ImprovedQuantileLightGBM(quantiles=self.quantiles)
        self.model.fit(
            splits['X_train'], splits['y_train'],
            splits['X_val'], splits['y_val'],
            sample_weights=training_weights,
            capacity_adjustment=capacity_adjustment
        )

        # Store feature importance
        if hasattr(self.model.models[0.5], 'feature_importances_'):
            self.feature_importance = pd.DataFrame({
                'feature': splits['feature_cols'],
                'importance': self.model.models[0.5].feature_importances_
            }).sort_values('importance', ascending=False)

        return splits

    def forecast_june_2025(self, atm_features: pd.DataFrame) -> pd.DataFrame:
        """
        Generate forecast for June 2025 with capacity adjustments.
        """
        # Prepare June 2025 data
        june_data = atm_features[
            (atm_features['DateTime'] >= '2025-06-01') &
            (atm_features['DateTime'] < '2025-07-01')
        ].copy()

        june_data = june_data.set_index('DateTime')
        june_data = self._add_solar_weather_features(june_data)

        # Add capacity features for June 2025
        # Use 2025 projections for capacity features
        june_data['year'] = 2025
        # Create timezone-aware timestamp if needed
        start_timestamp = pd.Timestamp('2022-01-01')
        if june_data.index.tz is not None:
            start_timestamp = start_timestamp.tz_localize(june_data.index.tz)
        june_data['days_since_start'] = (june_data.index - start_timestamp).days
        june_data['years_since_start'] = june_data['days_since_start'] / 365.25

        # Project capacity features to 2025
        if self.capacity_growth_rate is not None:
            reference_capacity = 15000  # Approximate 2022 baseline
            june_data['exp_capacity_trend'] = reference_capacity * np.exp(
                self.capacity_growth_rate * (2025 - 2022)
            )
            june_data['linear_capacity_trend'] = june_data['days_since_start'] * 10  # Simplified
            june_data['capacity_growth_rate'] = self.capacity_growth_rate

            # Seasonal adjustment
            month_factors = {6: 1.2}  # June peak season
            june_data['seasonal_capacity_factor'] = 1.2
            june_data['adjusted_capacity_trend'] = (
                june_data['exp_capacity_trend'] * june_data['seasonal_capacity_factor']
            )
            june_data['is_recent'] = 1  # All 2025 data is "recent"
            june_data['capacity_utilization'] = 0.8  # Assumed value

            # Year dummies
            june_data['year_2025'] = 1
            for year in [2022, 2023, 2024]:
                june_data[f'year_{year}'] = 0

        # Select features used in training
        X_june = june_data[self.model.models[0.5].feature_name_]

        # Generate predictions
        predictions = self.model.predict(X_june, apply_capacity_adjustment=True)

        # Create submission dataframe
        submission_df = pd.DataFrame({
            'DateTime': X_june.index,
            'power': predictions['q50']
        })

        # Add probabilistic predictions
        for q_str, pred in predictions.items():
            if q_str != 'q50':
                submission_df[f'power_{q_str}'] = pred

        return submission_df

def evaluate_model_performance(y_true, predictions, model_name="Model"):
    """
    Comprehensive model evaluation with focus on capacity bias.
    """
    metrics = {}

    # Point forecast metrics (using median)
    if 'q50' in predictions:
        y_pred = predictions['q50']

        metrics['RMSE'] = np.sqrt(mean_squared_error(y_true, y_pred))
        metrics['MAE'] = mean_absolute_error(y_true, y_pred)
        metrics['R²'] = r2_score(y_true, y_pred)

        # Bias analysis
        metrics['Mean_Bias'] = np.mean(y_pred - y_true)
        metrics['Median_Bias'] = np.median(y_pred - y_true)

        # Bias by time of day (to check for systematic capacity underestimation)
        if hasattr(y_true, 'index'):
            hours = y_true.index.hour
            hourly_bias = []
            for hour in range(24):
                hour_mask = hours == hour
                if hour_mask.sum() > 0:
                    hour_bias = np.mean(y_pred[hour_mask] - y_true[hour_mask])
                    hourly_bias.append(hour_bias)

            metrics['Peak_Hours_Bias'] = np.mean(hourly_bias[10:16])  # 10 AM to 4 PM
            metrics['Night_Hours_Bias'] = np.mean(hourly_bias[20:] + hourly_bias[:6])  # 8 PM to 6 AM

    print(f"\n{model_name} Performance Metrics:")
    for metric, value in metrics.items():
        print(f"  {metric}: {value:.3f}")

    return metrics

if __name__ == "__main__":
    # Example usage
    print("Testing improved solar forecasting pipeline...")

    # Load data
    solar_data = pd.read_csv('/home/tenantadmin/tsf-solar/data/germany_solar_observation_q1.csv')
    solar_data['DateTime'] = pd.to_datetime(solar_data['DateTime'])

    atm_data = pd.read_csv('/home/tenantadmin/tsf-solar/data/germany_atm_features_q1.csv')
    atm_data['DateTime'] = pd.to_datetime(atm_data['DateTime'])

    # Initialize pipeline
    pipeline = CapacityAwareForecastingPipeline()

    # Prepare features
    print("Preparing features with capacity trends...")
    data = pipeline.prepare_features(solar_data, atm_data)

    print(f"Enhanced dataset shape: {data.shape}")
    print(f"Capacity growth rate: {pipeline.capacity_growth_rate:.3f}")

    # Fit model
    print("Training capacity-aware model...")
    splits = pipeline.fit(data)

    # Evaluate on validation set
    val_predictions = pipeline.model.predict(splits['X_val'])
    metrics = evaluate_model_performance(splits['y_val'], val_predictions, "Improved Model")

    # Generate June 2025 forecast
    print("Generating June 2025 forecast...")
    june_forecast = pipeline.forecast_june_2025(atm_data)

    print(f"\nJune 2025 forecast summary:")
    print(f"Mean predicted power: {june_forecast['power'].mean():.2f} MWh")
    print(f"Max predicted power: {june_forecast['power'].max():.2f} MWh")
    print(f"Total monthly generation: {june_forecast['power'].sum():.0f} MWh")

    # Save results
    june_forecast[['DateTime', 'power']].to_csv('improved_forecast_q1.csv', index=False)
    june_forecast.to_csv('improved_forecast_q1_probabilistic.csv', index=False)

    print("Improved forecasts saved!")
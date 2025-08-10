# Case Study

## Question 1 - Solar Power Generation Forecasting
Objective:
Your task is to build a machine learning model to forecast Germany’s hourly solar power
generation for the entire month of June 2025.
To do so, you are provided with:



- Hourly grid-scale observed solar power generation data for Germany from 2022-0101 to 2025-05-31 in MWh (germany_solar_observation_q1.csv)
- Hourly, country-aggregated meteorological variables for Germany from 2022-01-01
to 2025-06-30 23:00:00 (germany_atm_features_q1.csv)

Data Provided:
1. Solar Power Generation Data
File:
Description: Hourly grid-scale solar
Time span: 2022-01-01 to 2025-05-31

power

germany_solar_observation_q1.csv
generation in Germany (MWh)

2. Meteorological Features
File:
germany_atm_features_q1.csv
Description: Hourly country-aggregated meteorological variables from 2022-01-01 to 202506-30
23:00:00
These variables are aggregated based on solar power plant geolocations weighted by their
installed capacity. Variables include:
DateTime: Hourly timestamp in UTC
surface_solar_radiation_downwards: Global horizontal irradiance (W/m²)
temperature_2m: Temperature at 2m height (°C)
total_cloud_cover: Total cloud cover (%)
total_precipitation: Total precipitation (mm)

snowfall: Snowfall (mm)
snow_depth: Snow depth (mm)
wind_speed_10m: Wind speed at 10m height (m/s)
wind_speed_100m: Wind speed at 100m height (m/s)
apparent_temperature: Feels-like temperature (°C)
relative_humidity_2m: Relative humidity at 2m height (%)
power: Solar power generation (MWh).
Instructions:
•
•
•

Develop a machine learning model to predict hourly solar power generation for
Germany for the period 2025-06-01 00:00 to 2025-06-30 23:00.
You may use any libraries for data processing, feature engineering, and modeling.
You are encouraged to perform additional feature engineering and you are free to
enrich the data.

### Deliverables:
forecast_q1.csv: with columns DateTime, power
DateTime column should be in UTC time zone and the output should be hourly
Well-documented and reproducible source code (R or Python)
A short report (max 2–3 pages) including:
- Model(s) used and training/validation strategy
- Feature engineering
- Plots of model performance on validation data
- Optional: Feature importance or interpretation

## Question 2 – Detecting True Demand Growth in a Solar-Rich Energy System
Background:
In recent years, Germany has seen a significant rise in rooftop solar panel installations. A
large share of this generation is consumed locally and does not appear in grid-scale solar
generation statistics. This type of production is known as behind-the-meter (BTM) solar
generation.
As a result, during sunny hours — especially around midday — the grid-observed electricity
demand appears lower than it actually is, because part of the demand is being met directly
by local solar production. This makes it difficult to assess the true level of electricity
consumption and the impact of distributed solar generation.

Objective:
Your task is to estimate the year-over-year true demand growth in Germany from 2020 to
today, by removing the effect of behind-the-meter solar generation.
You will be provided with:
Hourly electricity demand data from Germany (2020–present) (demand, MWh)
(germany_electricity_demand_observation_q2.csv)
Country-aggregated hourly meteorological variables (see Part 1 for descriptions)
(germany_atm_features_q2.csv). These variables are aggregated based on the
geolocations of the cities in Germany weighted by their population.
Grid-scale
solar
power
generation
(power,
MWh)
(germany_solar_observation_q2.csv)

Guiding Questions
1. How can you isolate the effect of BTM solar generation on observed demand?
2. Can you estimate what the demand would have been during solar hours if there
were no rooftop solar?
3. Estimate the difference between projected demand and observed demand.
4. How has BTM solar generation evolved from 2020 to the present, based on your
analysis?
### Deliverables

Well-documented and reproducible source code (R or Python)
A short report including an answer to all the guiding questions.

## Question 3 – GFS Forecast Data Extraction & Analysis
You are asked to build a small data pipeline and analysis workflow using NOAA GFS forecast
data, publicly available.
Tasks:
1. Data Subset Extraction

a. Build a system that downloads NOAA GFS forecast data, which is published 4
times a day (00, 06, 12, 18Z).
b. Extract only the first 72 hours (0–72h) of each forecast run.
c. Subset only variables required to calculate wind power density at 100-meter
height and 2 meter temperature.
d. Focus on the European region subset of the data.
2. Calculate Wind Power Density
a. Compute wind power density at 100 meters above ground for the entire
European region.
b. Visualize daily average wind power density maps, with each day in a separate
facet.
3. Country Ranking by Wind Power Density
a. For each 3-day forecast run, compute country-level average wind power
density.
b. Rank countries from highest to lowest wind power potential.
### Deliverables:


Visualization and analyses for each task.
The complete code used for data extraction, analysis, and visualization.
Ensure the code and explanations provided allow for reproducibility of the results.
All tasks of this question (except for the first "Data Subset Extraction") must be
completed using the R programming language.
You may optionally include Python implementations in addition to the required R
code, but R is mandatory for evaluation.

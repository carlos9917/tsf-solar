# How to Run the GFS Wind Power Density Pipeline

This document provides instructions on how to set up and run the GFS Wind Power Density Pipeline.

## 1. Project Structure

The project is organized into the following directories:

- `config/`: Contains the configuration file `config.py`, where you can set parameters such as database paths, logging levels, and API keys.
- `data/`: This directory is used to store raw and processed data.
  - `raw/`: Contains the raw GRIB files downloaded from the GFS.
  - `processed/`: Contains the processed data, including the SQLite database (`gfs_data.db`) and generated plots.
- `src/`: Contains the source code for the pipeline.
  - `data_extractor.py`: The Python script for downloading and processing GFS data.
  - `analysis.R`: The R script for analyzing the processed data and generating visualizations.
  - `scheduler.py`: The Python script for scheduling the data extraction and analysis.
  - `dashboard.py`: The Python script for the interactive dashboard.
  - `dashboard.R`: The R script for the interactive dashboard.
- `logs/`: Contains the log files for the pipeline.

## 2. Installation

To set up the project, follow these steps:

1. **Clone the repository:**
   ```bash
   git clone <repository_url>
   cd <repository_name>
   ```
2. **Install the required Python packages:**
   ```bash
   pip install -r requirements.txt
   ```
3. **Install the required R packages:**
   The `analysis.R` script will automatically install any missing R packages when it's run for the first time.

## 3. Running the Pipeline

The `run.sh` script provides a simple interface for running the pipeline.

### 3.1. Running the Scheduler

To run the scheduler, which will automatically download and process new GFS data every 6 hours, use the following command:

```bash
./run.sh scheduler
```

The scheduler will run in the foreground, and you can stop it by pressing `Ctrl+C`.

### 3.2. Running the R Shiny Dashboard

To run the interactive R Shiny dashboard, use the following command:

```bash
./run.sh dashboard
```

The dashboard will be available at `http://127.0.0.1:8050`.

### 3.3. Running the Python Dash Dashboard

To run the interactive Python Dash dashboard, use the following command:

```bash
./run.sh dashboard-py
```

The dashboard will be available at `http://127.0.0.1:8050`.

### 3.4. Manual Data Extraction

To run a manual data extraction for a specific date and cycle, use the following command:

```bash
./run.sh manual --date <YYYYMMDD> --cycle <00|06|12|18>
```

For example, to extract data for August 7, 2025, cycle 12, you would run:

```bash
./run.sh manual --date 20250807 --cycle 12
```

## 4. Viewing the Output

The output of the pipeline is stored in the `data/processed` directory.

- `gfs_data.db`: The SQLite database containing the processed GFS data.
- `plots/`: Contains the generated plots, including wind power density maps and country rankings.

You can also view the output through the interactive dashboard.

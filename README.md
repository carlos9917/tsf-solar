# TSF-Solar: Time Series Forecasting for Solar Energy

A machine learning project for forecasting Germany's hourly solar power generation using meteorological data. 
This project implements multiple forecasting models including traditional ML approaches (Ridge Regression, XGBoost) and deep learning methods (Feedforward Neural Networks, LSTM).

## 📋 Project Overview

This repository contains a solution for a case study on solar power forecasting. The main objective is to develop accurate machine learning models to predict Germany's hourly solar power generation for June 2025 using historical data and meteorological features.

## 🏗️ Repository Structure

```
tsf-solar/
│
├── data/                   # Raw and processed data (gitignored)
│   ├── raw/               # Original datasets
│   └── processed/         # Cleaned and feature-engineered data
│
├── notebooks/             # Analysis notebooks
│   ├── eda_question_1.qmd           # Exploratory Data Analysis
│   ├── eda_question_2.qmd           # Exploratory Data Analysis
│   └── modelling_question_1.qmd     # Model development and comparison
│   └── modelling_question_2.qmd     # Model development and comparison
│
├── src/                   # Source code modules
│   ├── __init__.py
│   ├── models.py          # PyTorch Lightning model classes
│   ├── utils.py           # Utility functions
│   └── data_pipeline.py   # Data loading and preprocessing
│
├── outputs/               # Generated results (gitignored)
│   ├── forecast_q1.csv    # Final predictions
│   └── logs/              # Training logs
│
├── doc/                   # Documentation
│   └── CaseStudyQuestions.md
│
├── requirements.txt       # Python dependencies
├── README.md             # This file
├── .gitignore           # Git ignore rules
```

## 🚀 Quick Start

### Prerequisites
- Python 3.11+
- Linux/macOS (tested on Linux)
- Quarto CLI (for rendering notebooks)
- R version 4.5.1

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/carlos9917/tsf-solar.git
   cd tsf-solar
   ```

2. **Set up virtual environment with uv**
   ```bash
   # Install uv if not already installed
   curl -LsSf https://astral.sh/uv/install.sh | sh

   # Create and activate virtual environment
   uv venv .venv --python 3.11
   source .venv/bin/activate

   # Install dependencies
   uv pip install -r requirements.txt
   ```

3. **Install Quarto** (if not already installed)
   - Follow instructions at: https://quarto.org/docs/get-started/


4. **Install R and R Libraries**
   A working installation of R (version 4.5.1 or newer) is required to run the R-based analysis scripts and notebooks.

   - **For the `pipeline_example`:** The R packages are installed automatically when you run the `analysis.R` script, as mentioned in the [pipeline how-to guide](./pipeline_example/howto.md).
   - **For other R scripts (`.qmd` files in `doc/`):** You will need to install the required libraries manually. You can do this by opening an R session and running:
     ```R
     install.packages(c("tidyverse", "lubridate", "plotly", "skimr", "arrow", "lwgeom"))
     ```
   To ensure environment isolation, you may consider using a tool like `renv`.

### Data Setup

1. Place your data files in the `data/raw/` directory:
   - `germany_atm_features_q1.csv`
   - `germany_solar_observation_q1.csv`

2. The notebooks will automatically load and process the data.

## 📊 Usage

### Running the Analysis

1. **Exploratory Data Analysis**
   ```bash
   quarto render notebooks/eda_question_1.qmd
   quarto render notebooks/eda_question_2.qmd
   ```

2. **Model Development and Forecasting**
   ```bash
   quarto render notebooks/modelling_question_1.qmd
   quarto render notebooks/modelling_question_2.qmd
   ```

3. **Answer to questions** 
   ```bash
   quarto render notebooks/answer_questio1.qmd
   quarto render notebooks/answer_questio2.qmd
   ```

3. **View Results**
   - Open the generated HTML files in your browser
   - Check `outputs/forecast_q1.csv` for final predictions

### Key Notebooks

- **`eda_question_1.qmd`**: Exploratory data analysis for question 1 including:
  - Data quality assessment
  - Time series visualization
  - Correlation analysis
  - Feature importance identification

- **`modelling_question_1.qmd`**: Modeling pipeline for question 1 featuring:
  - Feature engineering based on EDA insights
  - Seven different model implementations
  - Model comparison and evaluation
  - Model selection

- **`answer_question_1.qmd`**: Final answer to question 1.
  - Final forecast generation for June 2025

- **`modelling_question_2.qmd`**: Modeling pipeline for question 2 featuring:
  - Feature engineering based on EDA insights
  - Three different model implementations
  - Model ensemble based on the three previous models
  - Model selection

- **`answer_question_2.qmd`**: Final answer to question 2.
  - Answers to key questions

## 🤖 Models Tested

### 1. Persistence

### 2. Ridge Regression
- Simple linear model for interpretability
- Feature importance analysis
- Quick baseline performance

### 3. XGBoost
- Gradient boosting for tabular data
- Hyperparameter tuning with early stopping
- Feature importance ranking

### 4. Feedforward Neural Network 
- Multi-layer perceptron with dropout
- Learning rate scheduling
- Early stopping and checkpointing

### 5. LSTM
- Sequence modeling for temporal dependencies
- 24-hour lookback window
- Regularization and proper sequence handling

### 6.

### 7.

## 📈 Key Results

The models are evaluated using:
- **RMSE** (Root Mean Square Error)
- **MAE** (Mean Absolute Error)
- **R²** (Coefficient of Determination)

The best performing model is selected for answering the questions.

## 🔧 Technical Details

### Dependencies
- **Core**: pandas, numpy, scikit-learn
- **Visualization**: matplotlib, seaborn
- **ML**: xgboost
- **Deep Learning**: torch, pytorch-lightning
- **Notebooks**: quarto

### Hardware Requirements
- **CPU**: Multi-core recommended for XGBoost
- **RAM**: 8GB+ recommended
- **GPU**: Optional (PyTorch Lightning will auto-detect)

## 📁 Output Files

After running the notebooks, you'll find:

- `outputs/forecast_q1.csv`: Final hourly predictions for June 2025
- `outputs/logs/`: Training logs and model checkpoints
- HTML reports from rendered Quarto notebooks

## 📞 Contact

**Carlos Peralta**
- GitHub: [@carlos9917](https://github.com/carlos9917)
- Email: [carlos9917@gmail.com]

## 📦 Pipeline Example

This repository includes a sample data pipeline in the `pipeline_example/` directory. This pipeline demonstrates how to:
- Schedule and automate data extraction from a source (in this case, GFS data).
- Process the raw data into a structured format.
- Perform analysis and generate visualizations using both Python and R.

For detailed instructions on how to set up and run this example pipeline, please see the guide:
- [**How to Run the GFS Wind Power Density Pipeline**](./pipeline_example/howto.md)

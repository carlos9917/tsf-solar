# TSF-Solar: Time Series Forecasting for Solar Energy

A machine learning project for forecasting Germany's hourly solar power generation using meteorological data. 
This project implements multiple forecasting models including traditional ML approaches (Ridge Regression, XGBoost) and deep learning methods (Feedforward Neural Networks, LSTM).

## 📋 Project Overview

This repository contains the complete solution for the Vitus Commodities case study on solar power forecasting. The main objective is to develop accurate machine learning models to predict Germany's hourly solar power generation for June 2025 using historical data and meteorological features.

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
│   └── CaseStudy_VitusCommodities.pdf
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
   ```

2. **Model Development and Forecasting**
   ```bash
   quarto render notebooks/modelling_question_1.qmd
   ```

3. **View Results**
   - Open the generated HTML files in your browser
   - Check `outputs/forecast_q1.csv` for final predictions

### Key Notebooks

- **`eda_question_1.qmd`**: Comprehensive exploratory data analysis including:
  - Data quality assessment
  - Time series visualization
  - Correlation analysis
  - Feature importance identification

- **`modelling_question_1.qmd`**: Complete modeling pipeline featuring:
  - Feature engineering based on EDA insights
  - Four different model implementations
  - Model comparison and evaluation
  - Final forecast generation for June 2025

- **`modelling_question_1.qmd`**: Complete modeling pipeline featuring:
  - Feature engineering based on EDA insights
  - Four different model implementations
  - Model comparison and evaluation
  - Final forecast generation for June 2025

## 🤖 Models Implemented

### 1. Ridge Regression
- Simple linear model for interpretability
- Feature importance analysis
- Quick baseline performance

### 2. XGBoost
- Gradient boosting for tabular data
- Hyperparameter tuning with early stopping
- Feature importance ranking

### 3. Feedforward Neural Network 
- Multi-layer perceptron with dropout
- Learning rate scheduling
- Early stopping and checkpointing

### 4. LSTM
- Sequence modeling for temporal dependencies
- 24-hour lookback window
- Regularization and proper sequence handling

## 📈 Key Results

The models are evaluated using:
- **RMSE** (Root Mean Square Error)
- **MAE** (Mean Absolute Error)
- **R²** (Coefficient of Determination)

The best performing model is automatically selected for the final June 2025 forecast based on validation RMSE.

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

## pipeline_example

Contains an example data pipeline following guidelines
requested in Question 3.

Some comments about this approach
Although the procedure that asks for data to be
stored in 



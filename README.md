# TSF-Solar: Time Series Forecasting for Solar Energy

A machine learning project for forecasting Germany's hourly solar power generation using meteorological data. 
This project implements multiple forecasting models including traditional ML approaches (Ridge Regression, XGBoost) and deep learning methods (Feedforward Neural Networks, LSTM).

## 📋 Project Overview

This repository contains a solution for a case study on solar power forecasting. The main objective is to develop accurate machine learning models to predict Germany's hourly solar power generation for June 2025 using historical data and meteorological features.

## 🏗️ Repository Structure

```
tsf-solar/
│
├── data/                   # data provided
│
├── notebooks/             # Analysis notebooks
│   ├── eda_question_1.qmd           # Exploratory Data Analysis for question 1
│   ├── eda_question_2.qmd           # Exploratory Data Analysis for question 2
│   └── model_evaluation_question_1.qmd     # Model development and comparison for question 1
│   └── model_evaluation_question_2.qmd     # Model development and comparison for question 2
│   └── answer_question_1.qmd     # Answer to question 1
│   └── answer_question_2.qmd     # Answer to question 2
│   └── requirements.txt #requirements for venv (should work the same with requirements.txt in root dir)
│
├── pipeline_example/      # Example data pipeline (question 3)
│   ├── run.sh    # main script to run pipeline
│   ├── src    # data classes and scheduler
│   ├── config    # pipeline configuration file
│   ├── data    # data processing output
│   └── logs/              #  logs
│
├── doc/                   # questions
│   └── CaseStudyQuestions.md
│
├── requirements.txt       # Python dependencies for all notebooks
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
   A working installation of R (version 4.5.1 or newer) is required to run the R-based analysis scripts for the data pipeline.

   - **For the `pipeline_example`:** The R packages are installed automatically when you run the `analysis.R` script, as mentioned in the [pipeline how-to guide](./pipeline_example/howto.md).
   To ensure environment isolation for R, one may consider using a tool like `renv`.

## 📊 Usage

### Running the Analysis

0. **Install the virtual environment**
    
   ```bash
    cd notebooks
    uv venv .venv --python 3.11
    source .venv/bin/activate
    uv pip install -r requirements.txt
   ```
1. **Exploratory Data Analysis**
   ```bash
   quarto render notebooks/eda_question_1.qmd
   quarto render notebooks/eda_question_2.qmd
   ```

2. **Model Development and Forecasting**
   ```bash
   quarto render notebooks/model_evaluation_question_1.qmd #NOTE: might take 30-45 minutes...
   quarto render notebooks/model_evaluation_question_2.qmd
   ```

3. **Answer to questions** 
   ```bash
   quarto render notebooks/answer_question1.qmd
   quarto render notebooks/answer_question2.qmd
   ```

3. **View Results**
   - Open the generated HTML files in your browser
   - Check `forecast_q1.csv` and `forecast_q1_probabilistic.csv` for final predictions

### Key Notebooks

- **`eda_question_1.qmd`**: Exploratory data analysis for question 1 including:
  - Data quality assessment
  - Time series visualization
  - Correlation analysis
  - Feature importance identification

- **`eda_question_2.qmd`**: Exploratory data analysis for question 2 including:
  - Data quality assessment
  - Time series visualization
  - Correlation analysis
  - Feature importance identification

- **`model_evaluation_question_1.qmd`**: Modeling pipeline for question 1 featuring:
  - Feature engineering based on EDA insights
  - Seven different model implementations
  - Model comparison and evaluation
  - Model selection

- **`answer_question_1.qmd`**: Final answer to question 1.
  - Final forecast generation for June 2025

- **`model_evaluation_question_2.qmd`**: Modeling pipeline for question 2 featuring:
  - Feature engineering based on EDA insights
  - Three different model implementations
  - Model ensemble based on the three previous models
  - Model selection

- **`answer_question_2.qmd`**: Final answer to question 2.
  - Answers to key questions

## 🤖 Models Tested

### 1. Persistence

### 2. Ridge Regression

### 3. XGBoost

### 4. Feedforward Neural Network 

### 5. LSTM

### 6. LightGBM

### 7. Random Forest

## Model evaluation

The models are evaluated using:
- **RMSE** (Root Mean Square Error)
- **MAE** (Mean Absolute Error)
- **R²** (Coefficient of Determination)

The best performing model is selected for answering the questions.


## 📦 Pipeline Example

This repository includes a sample data pipeline in the `pipeline_example/` directory. This pipeline demonstrates how to:
- Schedule and automate data extraction from a source (in this case, GFS data).
- Process the raw data into a structured format.
- Perform analysis and generate visualizations using both Python and R.

For detailed instructions on how to set up and run this example pipeline, please see the guide:
- [**How to Run the GFS Wind Power Density Pipeline**](./pipeline_example/howto.md)

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

- `forecast_q1.csv` and `forecast_q1_probabilistic.csv`: Final hourly predictions for June 2025
- HTML reports from rendered Quarto notebooks

## 📞 Contact

**Carlos Peralta**
- GitHub: [@carlos9917](https://github.com/carlos9917)
- Email: [carlos9917@gmail.com]

#!/bin/bash

# run.sh

# This script provides a simple interface for running the GFS Wind Power Density Pipeline.

# Function to display help
show_help() {
    echo "Usage: ./run.sh [scheduler|dashboard|dashboard-py|manual|default] [options]"
    echo "  - scheduler: Run the automated data extraction and analysis pipeline."
    echo "  - dashboard: Run the interactive R Shiny dashboard."
    echo "  - dashboard-py: Run the interactive Python Dash dashboard."
    echo "  - manual: Run a one-time data extraction and analysis."
    echo "    - --date <YYYYMMDD>: The date to extract data for."
    echo "    - --cycle <00|06|12|18>: The GFS cycle to extract data for."
    echo "  - default: Run the original pipeline script."
}


# Activate virtual environment if it exists
if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
fi

# Get the first argument to determine the mode
MODE=$1
shift

# Display help if no arguments are provided
if [ -z "$MODE" ]; then
    show_help
    exit 0
fi


# --- Scheduler Mode ---
if [ "$MODE" == "scheduler" ]; then
    echo "Starting the scheduler..."
    python3 src/scheduler.py --mode scheduler
    
# --- R Shiny Dashboard Mode ---
elif [ "$MODE" == "dashboard" ]; then
    echo "Starting the R Shiny dashboard..."
    Rscript src/dashboard.R
    
# --- Python Dash Dashboard Mode ---
elif [ "$MODE" == "dashboard-py" ]; then
    echo "Starting the Python Dash dashboard..."
    python3 src/dashboard.py

# --- Manual Mode ---
elif [ "$MODE" == "manual" ]; then
    echo "Running manual data extraction..."
    python3 src/scheduler.py --mode manual "$@"
    
# --- Original Mode (for compatibility) ---
elif [ "$MODE" == "default" ]; then
    # Get today's date in YYYYMMDD format
    DATE=$(date +%Y%m%d)
    # Get the previous cycle (00, 06, 12, 18)
    # This is a simple example; a robust implementation would check for availability
    HOUR=$(date +%H)
    if (( HOUR >= 0 && HOUR < 6 )); then CYCLE="18"; DATE=$(date -d "yesterday" +%Y%m%d); fi

    if (( HOUR >= 6 && HOUR < 12 )); then CYCLE="00"; fi

    if (( HOUR >= 12 && HOUR < 18 )); then CYCLE="06"; fi

    if (( HOUR >= 18 )); then CYCLE="12"; fi

    echo "Running pipeline for date $DATE and cycle $CYCLE"

    # Run the Python data extraction script
    python3 src/data_extractor.py --date $DATE --cycle $CYCLE


    # Check if the python script succeeded
    if [ $? -eq 0 ]; then
      echo "Python script finished successfully."
      echo "Running R analysis script..."
      # Run the R analysis script
      Rscript src/analysis.R $DATE $CYCLE
      if [ $? -eq 0 ]; then
        echo "R script finished successfully."
      else
        echo "R script failed. Halting pipeline."
        exit 1
      fi
    else
      echo "Python script failed. Halting pipeline."
      exit 1
    fi

    echo "Pipeline finished."

# --- Help ---
else
    show_help
fi




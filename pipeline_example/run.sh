#!/bin/bash

# Activate virtual environment
source .venv/bin/activate

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
  Rscript analysis.R $DATE $CYCLE
else
  echo "Python script failed. Halting pipeline."
  exit 1
fi

echo "Pipeline finished."

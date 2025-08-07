#!/usr/bin/env bash
#
# run the scheduler
mkdir -p data/raw data/processed logs config src
python src/scheduler.py --date 20250803

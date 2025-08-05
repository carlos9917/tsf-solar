#!/usr/bin/env bash

# This script renders the Quarto notebooks for the case study.

echo "Rendering notebook for Question 1 (Python)..."
quarto render eda_question_1.py.qmd --to html

echo "Rendering notebook for Question 2 (Python)..."
quarto render eda_question_2.py.qmd --to html

echo "Done."

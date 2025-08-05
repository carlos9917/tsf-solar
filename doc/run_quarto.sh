#!/usr/bin/env bash

# This script renders the Quarto notebooks for the case study.

echo "Rendering notebook for Question 1..."
quarto render eda_question_1.qmd --to html

echo "Rendering notebook for Question 2..."
quarto render eda_question_2.qmd --to html

echo "Done."
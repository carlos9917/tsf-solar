#!/usr/bin/env bash

# This script renders the Quarto notebooks for the case study.

echo "Rendering EDA notebook for Question 1..."
quarto render eda_question_1.qmd --to html

echo "Rendering modelling notebook for Question 1..."
quarto render model_evaluation_question_1.qmd --to html

echo "Rendering answer notebook for Question 1..."
quarto render answer_question_1.qmd --to html

echo "Rendering EDA notebook for Question 2..."
quarto render eda_question_2.qmd --to html

echo "Rendering modelling notebook for Question 2..."
quarto render model_evaluation_question_2.qmd --to html

echo "Rendering answer notebook for Question 2..."
quarto render answer_question_2.qmd --to html
echo "Done."

# convert to pdf
#quarto render eda_question_1.py.qmd --to pdf
#quarto render eda_question_2.py.qmd --to pdf

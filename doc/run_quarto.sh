#!/usr/bin/env bash

NBOOK=carra_quarto_example_west.qmd
NBOOK=carra_quarto_example_means.qmd
NBOOK=$1
#create the html
quarto preview $NBOOK --no-browser --no-watch-inputs

#can also use
#quarto render $NBOOK 
#quarto render $NBOOK --to pdf


#convert to ipynb
#quarto convert $NBOOK

#!/usr/bin/env
#
## Commands to install the current environment
#
uv venv .venv --python 3.11
source .venv/bin/activate
uv pip install -r requirements.txt


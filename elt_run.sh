#!/bin/bash

# Virtual Environment Path
VENV_PATH="/home/laode/pacmann/project/orchestrate-elt-with-luigi/.venv/bin/activate"

# Activate venv
source "$VENV_PATH"

# set python script
PYTHON_SCRIPT="/home/laode/pacmann/project/orchestrate-elt-with-luigi/elt_main.py"

# run python script
python3 "$PYTHON_SCRIPT"
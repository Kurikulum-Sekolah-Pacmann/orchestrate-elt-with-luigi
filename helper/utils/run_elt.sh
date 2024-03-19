#!/bin/bash

# Virtual Environment Path
VENV_PATH="/home/laode/pacmann/project/orchestrate-elt-with-luigi/.venv/bin/activate" # Adjust with your path

# Activate Virtual Environment
source "$VENV_PATH"

# Run luigi visualizer
luigid --port 8082 &

# Set Python script
PYTHON_SCRIPT="/home/laode/pacmann/project/orchestrate-elt-with-luigi/elt_main.py" # Adjust with your path

# Run Python Script and insert log
python3 "$PYTHON_SCRIPT" &
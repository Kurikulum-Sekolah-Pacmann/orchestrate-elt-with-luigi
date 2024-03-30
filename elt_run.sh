#!/bin/bash

echo "========== Start Orcestration Process =========="

# Virtual Environment Path
VENV_PATH="/home/laode/pacmann/project/orchestrate-elt-with-luigi/.venv/bin/activate"

# Activate Virtual Environment
source "$VENV_PATH"

# Set Python script
PYTHON_SCRIPT="/home/laode/pacmann/project/orchestrate-elt-with-luigi/elt_main.py"

# Run Python Script 
python "$PYTHON_SCRIPT"

echo "========== End of Orcestration Process =========="
#!/bin/bash

# Run docker compose
DOCKER_COMPOSE__PATH="/home/laode/pacmann/project/orchestrate-elt-with-luigi/docker-compose.yaml" # Adjust with your path
docker compose -f $DOCKER_COMPOSE__PATH up -d

# Sleep for 5 seconds
sleep 5

# Virtual Environment Path
VENV_PATH="/home/laode/pacmann/project/orchestrate-elt-with-luigi/.venv/bin/activate" # Adjust with your path

# Activate Virtual Environment
source "$VENV_PATH"

# Install Requirements
REQUIREMENTS_PATH="/home/laode/pacmann/project/orchestrate-elt-with-luigi/requirements.txt" # Adjust with your path
pip install -r "$REQUIREMENTS_PATH"

# Init Data warehouse
DWH_INIT__PATH="/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline/utils/dwh_init.py" # Adjust with your path
python3 "$DWH_INIT__PATH"
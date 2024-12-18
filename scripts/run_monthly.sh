#!/bin/bash

# Directory containing the Python script
SCRIPT_DIR="$HOME/pull-gfm/"

# Navigate to the directory
cd "$SCRIPT_DIR" || exit 1

# Activate Python virtual environment
source .venv/bin/activate

# Get current date components
# date +%Y returns year in YYYY format (e.g., 2024)
# date +%m returns month in MM format (e.g., 01-12)
current_year=$(date +%Y)
current_month=$(date +%m)

# Validate year format
if [[ ! $current_year =~ ^[0-9]{4}$ ]]; then
    echo "Error: Invalid year format. Expected YYYY format, got: $current_year"
    exit 1
fi

# Calculate previous month 
if [ "$current_month" = "01" ]; then
    prev_month="12"
    year=$((current_year - 1))
else
    prev_month=$(printf "%02d" $((10#$current_month - 1)))
    year=$current_year
fi

echo "Running script for month: $prev_month, year: $year"

python filter_gfm.py --month "$prev_month" --year "$year"

# Deactivate venv
deactivate

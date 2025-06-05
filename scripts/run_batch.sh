#!/bin/bash

# Directory containing the Python script
SCRIPT_DIR="$HOME/gfm-puller/"
# Logs directory
LOGS_DIR="${SCRIPT_DIR%/}/logs"

# Create logs directory if it doesn't exist
mkdir -p "$LOGS_DIR"

# Navigate to the script directory
cd "$SCRIPT_DIR" || exit 1

# Activate Python virtual environment
source .venv/bin/activate

# Function to validate date format (MM-YYYY)
validate_date() {
    if ! [[ $1 =~ ^[0-1][0-9]-[0-9]{4}$ ]]; then
        echo "Error: Invalid date format. Use MM-YYYY (e.g., 01-2024)"
        exit 1
    fi
    
    # Remove leading zero for comparison
    month=${1%-*}
    month=$((10#$month)) # Force base-10 interpretation
    if [ "$month" -lt 1 ] || [ "$month" -gt 12 ]; then
        echo "Error: Month must be between 01 and 12"
        exit 1
    fi
}

# Function to get next month and year
get_next_month() {
    local month=$((10#$1)) # Force base-10 interpretation
    local year=$2
    
    if [ "$month" -eq 12 ]; then
        month=1
        year=$((year + 1))
    else
        month=$((month + 1))
    fi
    
    # Ensure month is padded with leading zero if needed
    printf "%02d-%04d" "$month" "$year"
}

# Check if correct number of arguments
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 START_DATE END_DATE"
    echo "Date format: MM-YYYY (e.g., 01-2024)"
    exit 1
fi

start_date=$1
end_date=$2

# Validate date formats
validate_date "$start_date"
validate_date "$end_date"

# Extract components and force base-10 interpretation
start_month=$((10#${start_date%-*}))
start_year=${start_date#*-}
end_month=$((10#${end_date%-*}))
end_year=${end_date#*-}

# Convert to comparable numbers (YYYYMM format)
start_num=$((start_year * 100 + start_month))
end_num=$((end_year * 100 + end_month))

# Check if start date is before end date
if [ "$start_num" -gt "$end_num" ]; then
    echo "Error: Start date must be before end date"
    exit 1
fi

# Create output filename in logs directory
output_file="$LOGS_DIR/batch_run_${start_date}--${end_date}.txt"

# Add header to the output file
{
    echo "Batch Processing Run: ${start_date} to ${end_date}"
    echo "Started at: $(date)"
    echo "----------------------------------------"
} > "$output_file"

# Process each month in the range
current_month=$start_month
current_year=$start_year
current_date=$(printf "%02d-%04d" "$current_month" "$current_year")

while [ $((current_year * 100 + current_month)) -le "$end_num" ]; do
    echo "Processing: $current_date"
    {
        echo -e "\nProcessing $current_date at $(date)"
        echo "----------------------------------------"
    } >> "$output_file"
    
    # Check AWS credentials before running filter_gfm
    if ! aws sts get-caller-identity >/dev/null 2>&1; then
        echo "Error: AWS credentials have expired. Please refresh and try again."
        exit 1
    fi
    
    # Format month with leading zero for the Python script
    formatted_month=$(printf "%02d" "$current_month")
    
    # Run Python script and redirect output to file
    "$SCRIPT_DIR/.venv/bin/python" filter_gfm.py --month "$formatted_month" --year "$current_year" >> "$output_file" 2>&1
    
    # Add separator
    echo "----------------------------------------" >> "$output_file"
    
    # Move to next month
    current_date=$(get_next_month "$current_month" "$current_year")
    current_month=${current_date%-*}
    current_month=$((10#$current_month)) # Force base-10 interpretation
    current_year=${current_date#*-}
done

# Add footer to the output file
echo -e "\nBatch processing completed at: $(date)" >> "$output_file"
echo "Batch processing complete. Output saved to: $output_file"

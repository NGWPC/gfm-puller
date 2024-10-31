#!/bin/bash
# Note: script needs to be run from the directory where the gfm data is!
# Find all JSON files and combine them into one
echo "Combining all JSON files into 'combined.json'..."
find . -name 'flood_fractions.json' -print0 | xargs -0 jq -s 'add' > combined.json

# Initialize the data file for Gnuplot
echo "Preparing data for plotting..."
echo "# Count Number_of_Values" > data.txt

# Loop from 0.01 to 1.00 in increments of 0.01
for count in $(seq 0.01 0.01 1.00); do
    # Count the number of values greater than or equal to the current count
    num_values=$(jq --argjson count "$count" '[.[] | tonumber] | map(select(. >= $count)) | length' combined.json)
    # Store the count and the number of values in 'data.txt'
    echo "$count $num_values" >> data.txt
done

# Plot the data using Gnuplot
echo "Plotting data with Gnuplot..."
gnuplot -persist <<-EOFMarker
    set terminal png size 1600,1200 enhanced font "arial,20"
    set output 'plot.png'
    set title '{/Bold Number of Subtiles With FB_{rat} Greater Than or Equal to Given Flood fraction}'
    set xlabel '{/Bold Flood to Baseline Ratio}'
    set ylabel '{/Bold Number of Subtiles}'
    set xtics font ",bold"
    set ytics font ",bold"
    set grid
    plot 'data.txt' using 1:2 with linespoints lw 2 title '{/Bold Tiles >= FB_{rat}}'
EOFMarker
echo "Plot saved as 'plot.png'."

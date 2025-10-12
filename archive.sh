#!/bin/bash

# Exit if parameter not provided
if [ -z "$1" ]; then
    echo "Error: YEAR_MONTH parameter required"
    echo "Usage: $0 YYYY-MM"
    exit 1
fi

YEAR_MONTH=$1
OUTPUT_FILE="${YEAR_MONTH//-/}.csv"

# Remove existing files
rm -f transactions.csv "$OUTPUT_FILE"

# Process first zip
FIRST_ZIP=$(ls -1 ${YEAR_MONTH}-*.zip 2>/dev/null | head -n 1)

if [ -z "$FIRST_ZIP" ]; then
    echo "No zip files found for $YEAR_MONTH"
    exit 1
fi

TOTAL_ROWS=0

echo "Processing first file: $FIRST_ZIP"
unzip -oq "$FIRST_ZIP"
cp transactions.csv "$OUTPUT_FILE"
ROWS=$(wc -l < transactions.csv)
TOTAL_ROWS=$((TOTAL_ROWS + ROWS))
echo "  Added $ROWS rows (Total: $TOTAL_ROWS)"

# Process remaining zips
for ZIP_FILE in $(ls -1 ${YEAR_MONTH}-*.zip | tail -n +2); do
    echo "Processing: $ZIP_FILE"
    unzip -oq "$ZIP_FILE"
    tail -n +3 transactions.csv >> "$OUTPUT_FILE"
    ROWS=$(tail -n +3 transactions.csv | wc -l)
    TOTAL_ROWS=$((TOTAL_ROWS + ROWS))
    echo "  Added $ROWS rows (Total: $TOTAL_ROWS)"
done

echo ""
echo "Done! Created $OUTPUT_FILE"
echo "Total rows: $TOTAL_ROWS"
echo "Final file size: $(wc -l < $OUTPUT_FILE) lines"

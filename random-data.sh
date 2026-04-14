#!/usr/bin/env bash

# Generates random Apache-style log lines for testing the Kusto logstash plugin.
# Usage: ./random-data.sh [output_file] [max_lines]
#   output_file: Path to write logs (default: /tmp/curllogs.txt)
#   max_lines:   Stop after this many lines; 0 = unlimited (default: 0)

OUTPUT_FILE="${1:-/tmp/curllogs.txt}"
MAX_LINES="${2:-0}"
COUNT=0

while true
do
    random_ip=$(dd if=/dev/urandom bs=4 count=1 2>/dev/null | od -An -tu1 | sed -e 's/^ *//' -e 's/  */./g')
    random_size=$(( (RANDOM % 65535) + 1 ))
    current_date_time=$(date '+%d/%b/%Y:%H:%M:%S %z')
    echo "$random_ip - - [$current_date_time] \"GET /data.php HTTP/1.1\" 200 $random_size" | tee -a "$OUTPUT_FILE"
    COUNT=$((COUNT + 1))
    if [ "$MAX_LINES" -gt 0 ] && [ "$COUNT" -ge "$MAX_LINES" ]; then
        echo "Reached $MAX_LINES lines, stopping."
        break
    fi
    sleep 0.1
done
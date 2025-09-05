#!/usr/bin/env bash
# you will need to install jq for JSON handling

while true
do
    # Generate random IP
    random_ip=$(dd if=/dev/urandom bs=4 count=1 2>/dev/null | od -An -tu1 | sed -e 's/^ *//' -e 's/  */./g')
    
    # Generate random response size and HTTP status
    random_size=$(( (RANDOM % 65535) + 1 ))
    status_codes=(200 201 400 404 500)
    random_status=${status_codes[$RANDOM % ${#status_codes[@]}]}

    # Generate current timestamp in ISO 8601
    timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

    # Random endpoint
    endpoints=("/api/data" "/user/login" "/metrics" "/products/123" "/health")
    random_endpoint=${endpoints[$RANDOM % ${#endpoints[@]}]}

    # Construct JSON log
    json_log=$(jq -c -n --arg ip "$random_ip" --arg ts "$timestamp" --arg endpoint "$random_endpoint" --arg status "$random_status" --arg size "$random_size" '{ip: $ip, timestamp: $ts, endpoint: $endpoint, status: ($status|tonumber), size: ($size|tonumber)}')


    echo "$json_log" | tee -a '/tmp/jsonlogs.txt'

    sleep 0.1
done

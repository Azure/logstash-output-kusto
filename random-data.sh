#!/usr/bin/env bash
while true
do
    random_ip=$(dd if=/dev/urandom bs=4 count=1 2>/dev/null | od -An -tu1 | sed -e 's/^ *//' -e 's/  */./g')
    random_size=$(( (RANDOM % 65535) + 1 ))
    current_date_time=$(date '+%d/%b/%Y:%H:%M:%S %z')
    echo "$random_ip - - [$current_date_time] \"GET /data.php HTTP/1.1\" 200 $random_size" | tee -a '/tmp/curllogs.txt'
    sleep 0.0000001s
done
#!/bin/bash
set -e

if [ -z "${STATION_ID}" ]; then
  ORDINAL=$(hostname | grep -o '[0-9]*$' || echo "0")
  export STATION_ID=$((ORDINAL + 1))
fi

echo "Launching weather station with STATION_ID=${STATION_ID}"
exec java -jar /app/app.jar

#!/bin/bash
set -e

# When deployed as a Kubernetes StatefulSet, the pod hostname ends in an
# ordinal (weather-station-0, weather-station-1, ...). Derive a 1-based
# STATION_ID from it unless STATION_ID was already provided explicitly
# (e.g. docker run -e STATION_ID=3, or docker-compose).
if [ -z "${STATION_ID}" ]; then
  ORDINAL=$(hostname | grep -o '[0-9]*$' || echo "0")
  export STATION_ID=$((ORDINAL + 1))
fi

echo "Launching weather station with STATION_ID=${STATION_ID}"
exec java -jar /app/app.jar

#!/bin/bash
set -e

# Check if data already exists
if [ -d "/data/gtfs" ] && [ "$(ls -A /data/gtfs)" ]; then
    echo "GTFS data already present, skipping download"
    exit 0
fi

echo "Downloading GTFS dataset..."

cd /tmp
curl -L --fail -o IDFM-gtfs.zip https://eu.ftp.opendatasoft.com/stif/GTFS/IDFM-gtfs.zip || exit 1

echo "Download completed, extracting..."
mkdir -p /data/gtfs
unzip -q IDFM-gtfs.zip -d /data/gtfs || exit 1

# Cleanup
rm IDFM-gtfs.zip

echo "GTFS data ready at /data/gtfs"
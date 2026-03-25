#!/bin/sh
set -e

if [ -d "/data/gtfs" ] && [ "$(ls -A /data/gtfs)" ]; then
    echo "GTFS data already present, skipping download"
    exit 0
fi

echo "Downloading GTFS dataset..."
cd /tmp
curl -L --fail -o IDFM-gtfs.zip https://eu.ftp.opendatasoft.com/stif/GTFS/IDFM-gtfs.zip

echo "Extracting..."
mkdir -p /data/gtfs
unzip -q IDFM-gtfs.zip -d /data/gtfs
rm IDFM-gtfs.zip

echo "Done"

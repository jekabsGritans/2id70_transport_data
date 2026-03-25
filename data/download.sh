#!/bin/bash
set -e

echo "Downloading GTFS dataset..."

cd /tmp
curl -L --fail -o IDFM-gtfs.zip https://eu.ftp.opendatasoft.com/stif/GTFS/IDFM-gtfs.zip

echo "Download completed, extracting..."
mkdir -p /data/gtfs
unzip -q IDFM-gtfs.zip -d /data/gtfs
rm IDFM-gtfs.zip

echo "GTFS data ready"
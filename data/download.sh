#!/bin/sh
set -e

if [ -d "/data/gtfs" ] && [ "$(ls -A /data/gtfs)" ]; then
    echo "GTFS data already present, skipping download"
    exit 0
fi

echo "Downloading GTFS dataset..."
URL="${GTFS_DOWNLOAD_URL:-https://eu.ftp.opendatasoft.com/stif/GTFS/IDFM-gtfs.zip}"
MAX_RETRIES="${GTFS_MAX_RETRIES:-12}"
SLEEP_SECONDS="${GTFS_RETRY_SLEEP_SECONDS:-10}"

attempt=1
while [ "$attempt" -le "$MAX_RETRIES" ]; do
    echo "Download attempt $attempt/$MAX_RETRIES"
    if GTFS_URL="$URL" python3 - <<'PY'
import pathlib
import os
import urllib.request
import zipfile

url = os.environ["GTFS_URL"]
zip_path = pathlib.Path("/tmp/IDFM-gtfs.zip")
target_dir = pathlib.Path("/data/gtfs")

target_dir.mkdir(parents=True, exist_ok=True)
print(f"Fetching {url}")
urllib.request.urlretrieve(url, zip_path)

print(f"Extracting to {target_dir}")
with zipfile.ZipFile(zip_path, "r") as zf:
    zf.extractall(target_dir)

zip_path.unlink(missing_ok=True)
PY
    then
        echo "Done"
        exit 0
    fi

    echo "Attempt $attempt failed; retrying in ${SLEEP_SECONDS}s..."
    attempt=$((attempt + 1))
    sleep "$SLEEP_SECONDS"
done

echo "Failed to download GTFS dataset after $MAX_RETRIES attempts"
exit 1

echo "Downloading dataset"

curl -L --fail -o IDFM-gtfs.zip https://eu.ftp.opendatasoft.com/stif/GTFS/IDFM-gtfs.zip || exit 1

# Sometimes (perhaps just on Windows) a blank character is at the end of the downloaded file, thus the double check
if [ -f IDFM-gtfs.zip ] || [ -f IDFM-gtfs.zip$'\r' ]; then
    echo "Download completed"
    unzip IDFM-gtfs.zip -d IDFM-gtfs || exit 1
else
    echo "Download failed"
    exit 1
fi

# Same as above
if [ -f IDFM-gtfs.zip ]; then
    rm IDFM-gtfs.zip
fi
if [ -f IDFM-gtfs.zip$'\r' ]; then
    rm IDFM-gtfs.zip$'\r'
fi
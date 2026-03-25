CREATE TABLE stops (
    stop_id TEXT PRIMARY KEY,
    stop_code TEXT,
    stop_name TEXT,
    stop_desc TEXT,
    stop_lon DOUBLE PRECISION,     
    stop_lat DOUBLE PRECISION,     
    zone_id TEXT,
    stop_url TEXT,
    location_type INTEGER,
    parent_station TEXT,
    stop_timezone TEXT,
    level_id TEXT,
    wheelchair_boarding INTEGER,
    platform_code TEXT,
    stop_access TEXT               -- Custom IDFM column
);

CREATE TABLE calendar (
    service_id TEXT PRIMARY KEY,
    monday INTEGER,
    tuesday INTEGER,
    wednesday INTEGER,
    thursday INTEGER,
    friday INTEGER,
    saturday INTEGER,
    sunday INTEGER,
    start_date INTEGER,
    end_date INTEGER
);

CREATE TABLE trips (
    route_id TEXT,
    service_id TEXT,
    trip_id TEXT PRIMARY KEY,
    trip_headsign TEXT,
    trip_short_name TEXT,
    direction_id INTEGER,
    block_id TEXT,
    shape_id TEXT,
    wheelchair_accessible INTEGER,
    bikes_allowed INTEGER
);

CREATE TABLE stop_times (
    trip_id TEXT,
    arrival_time INTERVAL,
    departure_time INTERVAL,
    start_pickup_drop_off_window TEXT, -- Custom IDFM column
    end_pickup_drop_off_window TEXT,   -- Custom IDFM column
    stop_id TEXT,
    stop_sequence INTEGER,
    pickup_type INTEGER,
    drop_off_type INTEGER,
    local_zone_id TEXT,
    stop_headsign TEXT,
    timepoint INTEGER,
    pickup_booking_rule_id TEXT,       -- Custom IDFM column
    drop_off_booking_rule_id TEXT      -- Custom IDFM column
);

COPY stops FROM '/data/gtfs/stops.txt' DELIMITER ',' CSV HEADER;
COPY calendar FROM '/data/gtfs/calendar.txt' DELIMITER ',' CSV HEADER;
COPY trips FROM '/data/gtfs/trips.txt' DELIMITER ',' CSV HEADER;
COPY stop_times FROM '/data/gtfs/stop_times.txt' DELIMITER ',' CSV HEADER;

CREATE INDEX idx_stop_times_trip ON stop_times(trip_id);
CREATE INDEX idx_stop_times_stop ON stop_times(stop_id);
CREATE INDEX idx_stop_times_seq ON stop_times(trip_id, stop_sequence);
CREATE INDEX idx_trips_service ON trips(service_id);
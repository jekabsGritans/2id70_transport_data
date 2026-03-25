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
CREATE TABLE calendar (
    service_id TEXT PRIMARY KEY,
    monday BOOLEAN,
    tuesday BOOLEAN,
    wednesday BOOLEAN,
    thursday BOOLEAN,
    friday BOOLEAN,
    saturday BOOLEAN,
    sunday BOOLEAN,
    start_date DATE,
    end_date DATE
);
CREATE TABLE calendar_dates (
    service_id TEXT,
    date DATE,
    exception_type INTEGER,
    PRIMARY KEY (service_id, date)
);

-- Staging tables for GTFS date columns
CREATE TEMP TABLE calendar_stage (
    service_id TEXT PRIMARY KEY,
    monday BOOLEAN,
    tuesday BOOLEAN,
    wednesday BOOLEAN,
    thursday BOOLEAN,
    friday BOOLEAN,
    saturday BOOLEAN,
    sunday BOOLEAN,
    start_date TEXT,
    end_date TEXT
);
CREATE TEMP TABLE calendar_dates_stage (
    service_id TEXT,
    date TEXT,
    exception_type INTEGER
);

COPY stop_times FROM '/data/gtfs/stop_times.txt' DELIMITER ',' CSV HEADER;
COPY trips FROM '/data/gtfs/trips.txt' DELIMITER ',' CSV HEADER;
COPY calendar_stage FROM '/data/gtfs/calendar.txt' DELIMITER ',' CSV HEADER;
COPY calendar_dates_stage FROM '/data/gtfs/calendar_dates.txt' DELIMITER ',' CSV HEADER;

INSERT INTO calendar
SELECT service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday,
    TO_DATE(start_date, 'YYYYMMDD'),
    TO_DATE(end_date,   'YYYYMMDD')
FROM calendar_stage;

INSERT INTO calendar_dates
SELECT service_id, TO_DATE(date, 'YYYYMMDD'), exception_type
FROM calendar_dates_stage;

-- TODO: myb add more
CREATE INDEX idx_stop_times_trip ON stop_times(trip_id);
CREATE INDEX idx_stop_times_stop ON stop_times(stop_id);
CREATE INDEX idx_stop_times_seq ON stop_times(trip_id, stop_sequence);
CREATE INDEX idx_trips_service ON trips(service_id);
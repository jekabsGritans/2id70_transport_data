/*
We only care about:

stop_times:
- trip_id TEXT
- stop_sequence INT
- stop_id TEXT
- arrival_time INTERVAL
- departure_time INTERVAL

trips:
- service_id TEXT

calendar:
- service_id TEXT
- monday, ..., sunday BOOLEAN
- start_date DATE
- end_date DATE

calendar_dates
- service_id TEXT
- date DATE
- exception_type INTEGER
*/


/*
arrival_dt, departure_dt are time interval types made by adding
time of day (as a time interval) + date (as time interval)

- create stop_times with associated calendar columns
- expand each row into multiple, based on (start_date, end_date, monday,...,sunday)
    (by a filtered join with a date_sequence table)
    - in the filtering - also separately use calendar dates
*/

/*
calendar-collapsed stop_times
*/
CREATE VIEW pivot_stop_times(
    trip_id, stop_sequence, service_id, stop_id, arrival_time, departure_time,
    monday, tuesday, wednesday, thursday, friday, saturday, sunday,
    start_date, end_date
) AS
SELECT
    t.trip_id, st.stop_sequence, t.service_id, st.stop_id, st.arrival_time, st.departure_time,
    c.monday, c.tuesday, c.wednesday, c.thursday, c.friday, c.saturday, c.sunday,
    c.start_date, c.end_date
FROM trips t
INNER JOIN stop_times st ON t.trip_id = st.trip_id
INNER JOIN calendar c ON t.service_id = c.service_id;

/*
calendar-collapsed edges
*/
CREATE VIEW pivot_edges(
    service_id, source_stop_id, target_stop_id, source_time, target_time,
    monday, tuesday, wednesday, thursday, friday, saturday, sunday,
    start_date, end_date
) AS
SELECT a.service_id, a.stop_id, b.stop_id, a.departure_time, b.arrival_time,
    a.monday, a.tuesday, a.wednesday, a.thursday, a.friday, a.saturday, a.sunday,
    a.start_date, a.end_date
FROM pivot_stop_times a
INNER JOIN pivot_stop_times b
ON a.trip_id = b.trip_id AND a.stop_sequence + 1 = b.stop_sequence;

/*
generate date sequence covering the full calendar range
*/
CREATE TABLE date_sequence AS
SELECT generate_series(
    (SELECT MIN(start_date) FROM calendar),
    (SELECT MAX(end_date) FROM calendar),
    INTERVAL '1 day'
)::DATE AS date;

CREATE INDEX idx_date_sequence ON date_sequence(date);

/*
exploded edges: one row per (edge, active date)
*/
CREATE VIEW time_edges AS
SELECT
    e.source_stop_id,
    e.target_stop_id,
    d.date + e.source_time AS departure_dt,
    d.date + e.target_time AS arrival_dt
FROM pivot_edges e
INNER JOIN date_sequence d
    ON d.date >= e.start_date
    AND d.date <= e.end_date
WHERE (
    CASE EXTRACT(DOW FROM d.date)::INTEGER
        WHEN 0 THEN e.sunday
        WHEN 1 THEN e.monday
        WHEN 2 THEN e.tuesday
        WHEN 3 THEN e.wednesday
        WHEN 4 THEN e.thursday
        WHEN 5 THEN e.friday
        WHEN 6 THEN e.saturday
    END
    AND NOT EXISTS (
        SELECT 1 FROM calendar_dates cd
        WHERE cd.service_id = e.service_id
          AND cd.date = d.date
          AND cd.exception_type = 2
    )
)
-- add dates with exception_type = 1 
OR EXISTS (
    SELECT 1 FROM calendar_dates cd
    WHERE cd.service_id = e.service_id
      AND cd.date = d.date
      AND cd.exception_type = 1
);
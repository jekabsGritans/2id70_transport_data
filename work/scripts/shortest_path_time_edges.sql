WITH RECURSIVE q(source_stop_id, target_stop_id, departure_dt, arrival_dt, hops, travel_time, path_nodes) AS (
  SELECT
    source_stop_id,
    target_stop_id,
    departure_dt,
    arrival_dt,
    1,
    (arrival_dt - departure_dt) AS travel_time,
    ARRAY[source_stop_id, target_stop_id]::text[]
  FROM time_edges
  WHERE source_stop_id = :SOURCE_STOP_ID
    AND departure_dt >= :START_DT

  UNION ALL

  SELECT
    q.source_stop_id,
    time_edges.target_stop_id,
    q.departure_dt,
    time_edges.arrival_dt,
    q.hops + 1,
    (time_edges.arrival_dt - q.departure_dt) AS travel_time,
    q.path_nodes || time_edges.target_stop_id
  FROM q
  JOIN time_edges
    ON time_edges.source_stop_id = q.target_stop_id
   AND time_edges.departure_dt >= q.arrival_dt
  WHERE q.hops < :MAX_HOPS
    AND NOT (time_edges.target_stop_id = ANY(q.path_nodes))
)
SELECT
  source_stop_id,
  target_stop_id,
  departure_dt,
  arrival_dt,
  travel_time,
  hops,
  path_nodes
FROM q
ORDER BY arrival_dt, target_stop_id;

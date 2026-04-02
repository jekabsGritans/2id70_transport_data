WITH RECURSIVE q(source_stop_id, target_stop_id, departure_dt, arrival_dt, hops, path_nodes) AS (
  SELECT
    source_stop_id,
    target_stop_id,
    departure_dt,
    arrival_dt,
    1,
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
    q.path_nodes || time_edges.target_stop_id
  FROM q
  JOIN time_edges
    ON time_edges.source_stop_id = q.target_stop_id
   AND time_edges.departure_dt >= q.arrival_dt
)
SELECT
  source_stop_id,
  target_stop_id,
  departure_dt,
  arrival_dt,
  hops,
  path_nodes
FROM q;

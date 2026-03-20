# Tell Logica to compile for PostgreSQL
@Engine("psql");

# Define an 'Edges' predicate
Edges(source_id:, target_id:, source_time:, target_time:) :-
    # Find a stop event...
    stop_times(trip_id: t, stop_id: source_id, departure_time: source_time, stop_sequence: seq1),
    
    # Find another stop event on the exact same trip (t)...
    stop_times(trip_id: t, stop_id: target_id, arrival_time: target_time, stop_sequence: seq2),
    
    # Where the second stop happens immediately after the first.
    seq2 == seq1 + 1;
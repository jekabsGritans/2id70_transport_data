@Engine('psql')

Edges(source_name:, target_name:) :-
    stop_times(trip_id: t, stop_id: sid1, stop_sequence: seq1),
    stop_times(trip_id: t, stop_id: sid2, stop_sequence: seq2),
    stops(stop_id: sid1, stop_name: source_name),
    stops(stop_id: sid2, stop_name: target_name),
    seq2 == seq1 + 1;

Reachable(source:, stop:, depth: 0) :-
    stops(stop_name: source),
    stop == source;

Reachable(source:, stop_b:, depth: d + 1) :-
    Reachable(source:, stop: stop_a, depth: d),
    Edges(source_name: stop_a, target_name: stop_b),
    ~Reachable(source:, stop: stop_b, depth:);

Shortest(source:, stop:) Min= depth :-
    Reachable(source:, stop:, depth:);
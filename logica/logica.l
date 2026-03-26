@Engine("psql");

Edges(source_id:, source_name:, target_id:, target_name:) distinct :-
    # building the graph
    # input(source_id:, dest_id: target_id),
    # source_id == "IDFM:monomodalStopPlace:46731",
    stop_times(trip_id: t, stop_id: source_id, stop_sequence: seq1),
    stops(stop_id: source_id, stop_name: source_name),
    stop_times(trip_id: t, stop_id: target_id, stop_sequence: seq2),
    stops(stop_id: target_id, stop_name: target_name),
    seq2 == seq1 + 1;

# Reachable(source:, stop:, depth: 0) :-
#     input(source_id: source_id),
#     stops(stop_id: source_id, stop_name: source_name),
#     stop == source;

# Reachable(source:, stop: stop_b, depth: d + 1) :-
#     Reachable(source:, stop: stop_a, depth: d),
#     Edges(source_name: stop_a, target_name: stop_b),
#     ~Reachable(source:, stop: stop_b, depth:);

# Shortest(source:, target:) Min= depth :-
#     Reachable(source:, stop: target, depth:);
@Engine("psql");

Edges(trip_id:, source_id:, target_id:, travel_time:, start_date:, end_date:, departure_time:, arrival_time:) distinct :-
    # find sequential stops (nodes connected by edges)
    stop_times(trip_id:, stop_id: source_id, stop_sequence: seq1, departure_time:),
    stop_times(trip_id:, stop_id: target_id, stop_sequence: seq2, arrival_time:),
    seq2 == seq1 + 1,
    travel_time == arrival_time - departure_time,
    trips(trip_id:, service_id:),
    calendar(service_id:, start_date:, end_date:, monday:, tuesday:, wednesday:, thursday:, friday:, saturday:, sunday:),
    input(departure_date:, departure_time: user_dt, day_of_week: dow),
    # only add edge to graph if it is in the available date range
    start_date <= departure_date,
    departure_date <= end_date,
    # only add edge to graph if it's departure time is after the time
    departure_time >= user_dt,
    # only add edges that have exception_type 1 or no exception
    (calendar_dates(service_id:, date: departure_date, exception_type: 1) | ~calendar_dates(service_id:, date: departure_date)),
    # only add edge to graph if it is available on the day
    # Day of Week: 0-6 (Mon-Sun) from Python
    (dow == 0 && monday == true) || (dow == 1 && tuesday == true) || (dow == 2 && wednesday == true) || (dow == 3 && thursday == true) || (dow == 4 && friday == true) || (dow == 5 && saturday == true) || (dow == 6 && sunday == true);

# base case: no edges taken
Distances(source:, target:, time_distance: SqlExpr("'0'::interval", {}), departure_time: SqlExpr("'0'::interval", {}), arrival_time: departure_time) distinct :-
    input(source_id: source, departure_time:),
    target == source;

# base case: first edge taken
Distances(source:, target: stop_b, time_distance: travel_time, departure_time: first_dep, arrival_time: edge_arr) distinct :-
    Distances(source:, target: stop_a, time_distance: SqlExpr("'0'::interval", {}), departure_time: SqlExpr("'0'::interval", {}), arrival_time: query_dep),
    Edges(source_id: stop_a, target_id: stop_b, departure_time: first_dep, arrival_time: edge_arr, travel_time:),
    first_dep >= query_dep;

# step case
Distances(source:, target: stop_b, time_distance: d + travel_time, departure_time:, arrival_time: at + travel_time) distinct :-
    Distances(source:, target: stop_a, time_distance: d, departure_time:, arrival_time: at),
    Edges(source_id: stop_a, target_id: stop_b, travel_time:);

# shortest distance for each node from source
Fastest(source:, target:, time_distance:, departure_time:, arrival_time:) :-
    Distances(source:, target:, time_distance:, departure_time:, arrival_time:);
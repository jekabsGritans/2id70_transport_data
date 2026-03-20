# dataset
- We use the open GTFS data from the Île-de-France region avaialble at https://transport.data.gouv.fr/resources/80921
- Run `download.sh` to download it.
- This dataset follows the [General Transit Feed Specification](https://gtfs.org/documentation/schedule/reference/) which defines some `.csv` file structures.

# setting up db
- `docker-compose.yml` has a postgres container  that uses `init.sql` to load data from `IDFM-gtfs/` (which you must first download with `download.sh` or manually) 

# querying db
- create a python venv and install the requirements `python -m venv venv && pip install -r requirements.txt`
- As a (vibe-coded) example, `run.py` compiles logica code in `edges.l` into sql and executes it.
    - This query creates the graph edges that we will need for pathfinding.
    


# todo
- [ ] GTFS does not include edges as a separate table. Maybe we should save ithem as a materialised view? or do we want to keep the table raw and include finding edges in the solution queries that we are benchmarking?
- [ ] Not all edges are active on all days. (the calendar table apparently specifies what is active when).
    - probably best to use this info by filtering trips for the day in mind as a first step

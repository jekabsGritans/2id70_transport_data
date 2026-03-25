# GTFS Transport Data

Uses the open GTFS data from the Île-de-France region ([IDFM dataset](https://transport.data.gouv.fr/resources/80921)), following the [General Transit Feed Specification](https://gtfs.org/documentation/schedule/reference/).

## Project Structure

```
transport_data/
├── data/          # Database container
│   ├── Dockerfile
│   ├── download.sh
│   └── init.sql
├── work/          # Querying/benchmarking container
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── logica/
│   └── scripts/
├── .devcontainer/
└── docker-compose.yml
```

- **`data/`** — PostgreSQL container. On first startup, automatically downloads the GTFS dataset and loads it into the database.
- **`work/`** — Python + Logica development environment. Connects to the database at `db:5432`.

## Setup

**Prerequisites:** Docker Desktop, VSCode + [Dev Containers](https://marketplace.visualstudio.com/items?itemName=ms-remote.remote-containers) extension.

1. Open the project in VSCode and click **"Reopen in Container"**
2. Docker Compose will build both containers and start them (the DB will download the dataset on first run)
3. Run scripts from the integrated terminal, e.g. `python scripts/run.py`

## Database

```
postgresql://myuser:mypassword@db:5432/gtfs_db
```

Tables: `stops`, `calendar`, `trips`, `stop_times`

## TODO
- [ ] GTFS does not include edges as a table — consider a materialised view?
- [ ] Not all edges are active on all days; filter trips by date using the `calendar` table

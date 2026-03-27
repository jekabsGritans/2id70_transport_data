import argparse
import os
from typing import Any

from sqlalchemy import create_engine, text


def load_sql(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Return all targets reachable from a source at/after a start time."
    )
    parser.add_argument("source_stop_id")
    parser.add_argument("start_dt", help="Example: 2026-03-26 08:00:00")
    parser.add_argument(
        "--db-url",
        default=os.environ.get(
            "DB_URL",
            "postgresql+psycopg2://myuser:mypassword@gtfs_postgres:5432/gtfs_db",
        ),
    )
    parser.add_argument(
        "--sql-file",
        default="work/scripts/shortest_path_time_edges.sql",
    )
    parser.add_argument(
        "--max-hops",
        type=int,
        default=8,
        help="Maximum recursion depth for reachability expansion.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    sql = load_sql(args.sql_file).rstrip().rstrip(";")
    if args.limit > 0:
        sql = f"{sql}\nLIMIT :RESULT_LIMIT"

    engine = create_engine(args.db_url)

    params: dict[str, Any] = {
        "SOURCE_STOP_ID": args.source_stop_id,
        "START_DT": args.start_dt,
        "MAX_HOPS": args.max_hops,
    }
    if args.limit > 0:
        params["RESULT_LIMIT"] = args.limit

    with engine.begin() as conn:
        rows = conn.execute(text(sql), params).mappings().all()

    if not rows:
        print("No reachable targets returned")
        return

    for i, row in enumerate(rows, start=1):
        print(f"--- result {i} ---")
        for key, value in dict(row).items():
            print(f"{key}: {value}")


if __name__ == "__main__":
    main()

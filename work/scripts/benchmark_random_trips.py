from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import random
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text

from run_shortest_path import load_sql


WORK_DIR = Path(__file__).resolve().parents[1]
DEFAULT_SQL_FILE = WORK_DIR / "scripts" / "shortest_path_time_edges.sql"
DEFAULT_LOGICA_FILE = WORK_DIR / "logica" / "logica.l"
DEFAULT_DB_URL = os.environ.get(
    "DB_URL",
    "postgresql+psycopg2://myuser:mypassword@db:5432/gtfs_db",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Sample random source stop IDs from stop_times and benchmark the SQL "
            "and Logica Fastest queries on the same random trips."
        )
    )
    parser.add_argument("--db-url", default=DEFAULT_DB_URL)
    parser.add_argument("--sql-file", default=str(DEFAULT_SQL_FILE))
    parser.add_argument("--logica-file", default=str(DEFAULT_LOGICA_FILE))
    parser.add_argument("--predicate", default="Fastest")
    parser.add_argument("--cases", type=int, default=25)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--max-hops", type=int, default=8)
    parser.add_argument(
        "--json-out",
        default=None,
        help="Optional path for a machine-readable JSON report.",
    )
    return parser.parse_args()


def compile_logica(predicate_name: str, file_path: str) -> str:
    result = subprocess.run(
        [sys.executable, "-m", "logica", file_path, "print", predicate_name],
        capture_output=True,
        text=True,
    )

    if result.returncode > 0:
        raise RuntimeError("\nLogica ran into a problem:\n" + result.stderr)

    return result.stdout.strip()


def split_compiled_logica(sql_query: str) -> tuple[str, str]:
    logica_setup_separator = "END $$;"
    parts = sql_query.rsplit(logica_setup_separator, 1)
    if len(parts) == 2:
        setup_sql = parts[0] + logica_setup_separator
        select_sql = parts[1].strip()
        return setup_sql, select_sql
    return "", sql_query.strip()


def load_stop_ids(conn) -> list[str]:
    rows = conn.execute(
        text(
            """
            SELECT DISTINCT stop_id
            FROM stop_times
            WHERE stop_id IS NOT NULL
            ORDER BY stop_id
            """
        )
    ).mappings().all()
    stop_ids = [row["stop_id"] for row in rows]
    if not stop_ids:
        raise RuntimeError("No stop IDs found in stop_times.")
    return stop_ids


def load_departure_bounds(conn) -> tuple[dt.timedelta, dt.timedelta]:
    row = conn.execute(
        text(
            """
            SELECT MIN(departure_time) AS min_time,
                   MAX(departure_time) AS max_time
            FROM stop_times
            WHERE departure_time IS NOT NULL
            """
        )
    ).mappings().one()

    min_time = row["min_time"] or dt.timedelta(0)
    max_time = row["max_time"] or dt.timedelta(hours=23, minutes=59, seconds=59)
    return min_time, max_time


def _is_active_service_date(conn, candidate_date: dt.date) -> bool:
    candidate_isodow = candidate_date.isoweekday()
    row = conn.execute(
        text(
            """
            SELECT (
                EXISTS (
                    SELECT 1
                    FROM calendar c
                    LEFT JOIN calendar_dates cdx
                      ON cdx.service_id = c.service_id
                     AND cdx.date = :candidate_date
                     AND cdx.exception_type = 2
                    WHERE c.start_date <= :candidate_date
                      AND :candidate_date <= c.end_date
                      AND (
                        (:candidate_isodow = 1 AND c.monday)
                        OR (:candidate_isodow = 2 AND c.tuesday)
                        OR (:candidate_isodow = 3 AND c.wednesday)
                        OR (:candidate_isodow = 4 AND c.thursday)
                        OR (:candidate_isodow = 5 AND c.friday)
                        OR (:candidate_isodow = 6 AND c.saturday)
                        OR (:candidate_isodow = 7 AND c.sunday)
                      )
                      AND cdx.service_id IS NULL
                    LIMIT 1
                )
                OR EXISTS (
                    SELECT 1
                    FROM calendar_dates
                    WHERE date = :candidate_date
                      AND exception_type = 1
                    LIMIT 1
                )
            ) AS is_active
            """
        ),
        {"candidate_date": candidate_date, "candidate_isodow": candidate_isodow},
    ).mappings().one()
    return bool(row["is_active"])


def _first_weekday_on_or_after(start_date: dt.date, end_date: dt.date, weekday: int) -> dt.date | None:
    delta = (weekday - start_date.weekday()) % 7
    candidate = start_date + dt.timedelta(days=delta)
    if candidate > end_date:
        return None
    return candidate


def load_active_calendar_date(conn) -> tuple[dt.date, int]:
    rows = conn.execute(
        text(
            """
            SELECT service_id, start_date, end_date,
                   monday, tuesday, wednesday, thursday,
                   friday, saturday, sunday
            FROM calendar
            WHERE start_date IS NOT NULL AND end_date IS NOT NULL
            ORDER BY random()
            LIMIT 200
            """
        )
    ).mappings().all()

    weekday_flags = [
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
    ]

    for row in rows:
        start_date = row["start_date"]
        end_date = row["end_date"]
        if start_date > end_date:
            continue
        enabled_weekdays = [
            weekday for weekday, flag in enumerate(weekday_flags) if row[flag]
        ]
        if not enabled_weekdays:
            continue
        random.shuffle(enabled_weekdays)
        for weekday in enabled_weekdays:
            candidate = _first_weekday_on_or_after(start_date, end_date, weekday)
            if candidate and _is_active_service_date(conn, candidate):
                return candidate, candidate.weekday()

    fallback = conn.execute(
        text(
            """
            SELECT date AS service_date
            FROM calendar_dates
            WHERE exception_type = 1
            ORDER BY random()
            LIMIT 1
            """
        )
    ).mappings().one_or_none()
    if fallback:
        service_date = fallback["service_date"]
        return service_date, service_date.weekday()

    raise RuntimeError("No active service date found in calendar/calendar_dates.")


def random_time_between(rng: random.Random, lower: dt.timedelta, upper: dt.timedelta) -> dt.timedelta:
    lower_seconds = int(lower.total_seconds())
    upper_seconds = int(upper.total_seconds())
    if upper_seconds < lower_seconds:
        lower_seconds, upper_seconds = upper_seconds, lower_seconds
    if upper_seconds == lower_seconds:
        return dt.timedelta(seconds=lower_seconds)
    return dt.timedelta(seconds=rng.randint(lower_seconds, upper_seconds))


def build_benchmark_cases(
    rng: random.Random,
    stop_ids: list[str],
    departure_date: dt.date,
    departure_time_bounds: tuple[dt.timedelta, dt.timedelta],
    case_count: int,
) -> list[dict[str, Any]]:
    lower_time, upper_time = departure_time_bounds
    cases: list[dict[str, Any]] = []

    for _ in range(case_count):
        source_stop_id = rng.choice(stop_ids)
        departure_time = random_time_between(rng, lower_time, upper_time)
        start_dt = dt.datetime.combine(departure_date, dt.time()) + departure_time
        cases.append(
            {
                "source_stop_id": source_stop_id,
                "departure_date": departure_date,
                "day_of_week": departure_date.weekday(),
                "departure_time": departure_time,
                "start_dt": start_dt.strftime("%Y-%m-%d %H:%M:%S"),
            }
        )

    return cases


def execute_sql_case(conn, sql_query: str, params: dict[str, Any]) -> tuple[list[dict[str, Any]], float]:
    started = time.perf_counter()
    rows = conn.execute(text(sql_query), params).mappings().all()
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    return [dict(row) for row in rows], elapsed_ms


def execute_logica_case(
    conn,
    select_sql: str,
    source_stop_id: str,
    departure_date: dt.date,
    day_of_week: int,
    departure_time: dt.timedelta,
) -> tuple[list[dict[str, Any]], float]:
    conn.execute(text("DELETE FROM input"))
    conn.execute(
        text(
            """
            INSERT INTO input (source_id, departure_date, day_of_week, departure_time)
            VALUES (:source_id, :departure_date, :day_of_week, :departure_time)
            """
        ),
        {
            "source_id": source_stop_id,
            "departure_date": departure_date,
            "day_of_week": day_of_week,
            "departure_time": departure_time,
        },
    )

    started = time.perf_counter()
    rows = conn.execute(text(select_sql)).mappings().all()
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    return [dict(row) for row in rows], elapsed_ms


def choose_sample_trip(rng: random.Random, rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None
    return rng.choice(rows)


def extract_target_id(row: dict[str, Any] | None) -> Any:
    if not row:
        return None
    return row.get("target") or row.get("target_id") or row.get("target_stop_id")


def sample_common_target_id(
    rng: random.Random,
    sql_rows: list[dict[str, Any]],
    logica_rows: list[dict[str, Any]],
) -> Any:
    sql_targets = {extract_target_id(row) for row in sql_rows}
    logica_targets = {extract_target_id(row) for row in logica_rows}
    common_targets = [target for target in (sql_targets & logica_targets) if target is not None]
    if not common_targets:
        return None
    return rng.choice(common_targets)


def main() -> None:
    print("Started program")
    args = parse_args()
    rng = random.Random(args.seed)

    engine = create_engine(args.db_url)
    sql_query = load_sql(args.sql_file).rstrip().rstrip(";")
    logica_sql = compile_logica(args.predicate, args.logica_file)
    setup_sql, select_sql = split_compiled_logica(logica_sql)
    print("Finished setup")
    benchmark_date = None
    benchmark_weekday = None
    case_rows: list[dict[str, Any]] = []
    print("Starting benchmark")
    with engine.connect() as conn:
        conn.execute(text("SET max_parallel_workers_per_gather = 0"))
        conn.execute(
            text(
                """
                CREATE TEMP TABLE input(
                    source_id VARCHAR,
                    departure_date DATE,
                    day_of_week INTEGER,
                    departure_time INTERVAL
                )
                """
            )
        )

        if setup_sql:
            conn.execute(text(setup_sql))
        stop_ids = load_stop_ids(conn)
        departure_bounds = load_departure_bounds(conn)
        benchmark_date, benchmark_weekday = load_active_calendar_date(conn)

        cases = build_benchmark_cases(
            rng=rng,
            stop_ids=stop_ids,
            departure_date=benchmark_date,
            departure_time_bounds=departure_bounds,
            case_count=args.cases,
        )
        for index, case in enumerate(cases, start=1):
            sql_params = {
                "SOURCE_STOP_ID": case["source_stop_id"],
                "START_DT": case["start_dt"],
                "MAX_HOPS": args.max_hops,
            }

            sql_rows, sql_ms = execute_sql_case(conn, sql_query, sql_params)
            logica_rows, logica_ms = execute_logica_case(
                conn,
                select_sql,
                case["source_stop_id"],
                case["departure_date"],
                case["day_of_week"],
                case["departure_time"],
            )

            common_sample_target = sample_common_target_id(rng, sql_rows, logica_rows)

            case_rows.append(
                {
                    "case": index,
                    "source_stop_id": case["source_stop_id"],
                    "departure_date": case["departure_date"].isoformat(),
                    "day_of_week": case["day_of_week"],
                    "departure_time": str(case["departure_time"]),
                    "sql_rows": len(sql_rows),
                    "sql_ms": round(sql_ms, 3),
                    "logica_rows": len(logica_rows),
                    "logica_ms": round(logica_ms, 3),
                    "sample_sql_target": common_sample_target,
                    "sample_logica_target": common_sample_target,
                }
            )

    sql_times = [row["sql_ms"] for row in case_rows]
    logica_times = [row["logica_ms"] for row in case_rows]

    report = {
        "db_url": args.db_url,
        "sql_file": str(Path(args.sql_file).resolve()),
        "logica_file": str(Path(args.logica_file).resolve()),
        "predicate": args.predicate,
        "cases": args.cases,
        "seed": args.seed,
        "max_hops": args.max_hops,
        "benchmark_date": benchmark_date.isoformat() if benchmark_date else None,
        "benchmark_weekday": benchmark_weekday,
        "summary": {
            "sql_mean_ms": round(statistics.mean(sql_times), 3) if sql_times else None,
            "sql_median_ms": round(statistics.median(sql_times), 3) if sql_times else None,
            "logica_mean_ms": round(statistics.mean(logica_times), 3) if logica_times else None,
            "logica_median_ms": round(statistics.median(logica_times), 3) if logica_times else None,
        },
        "results": case_rows,
    }

    print(f"Benchmarking {len(case_rows)} random trips using SQL and Logica Fastest...")
    print(f"Benchmark date: {report['benchmark_date']} (weekday {report['benchmark_weekday']})")
    print(
        "SQL:   mean {sql_mean_ms} ms | median {sql_median_ms} ms".format(
            **report["summary"]
        )
    )
    print(
        "Logica: mean {logica_mean_ms} ms | median {logica_median_ms} ms".format(
            **report["summary"]
        )
    )
    print()

    for row in case_rows[: min(10, len(case_rows))]:
        print(
            "Case {case}: {source_stop_id} @ {departure_date} {departure_time} | "
            "SQL {sql_ms} ms ({sql_rows} rows) | Logica {logica_ms} ms ({logica_rows} rows)".format(
                **row
            )
        )
        if row["sample_sql_target"] or row["sample_logica_target"]:
            print(
                f"  sample trip targets -> SQL: {row['sample_sql_target']} | Logica: {row['sample_logica_target']}"
            )

    if args.json_out:
        json_path = Path(args.json_out)
        json_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        print(f"\nWrote JSON report to {json_path}")


if __name__ == "__main__":
    main()

import subprocess
import datetime

import pandas as pd
import os
from sqlalchemy import create_engine, text

DATA_FOLDER: str = "./IDFM-gtfs"


def load_data():
    if (os.path.exists(DATA_FOLDER) and len(os.listdir(DATA_FOLDER)) > 0):
        print("Data already present")
        return

    try:
        subprocess.run(["sh", "scripts/download.sh"], check=True)
    except:
        print("Error while executing download.sh")
        exit(1)

def compile_logica(predicate_name: str, file_path: str):
    print(f"Compiling {predicate_name} from {file_path}...")
    result = subprocess.run(
        ['python', '-m', 'logica', file_path, 'print', predicate_name],
        capture_output=True, text=True
    )

    if result.returncode > 0:
        raise RuntimeError("\nLogica ran into a problem:\n" + result.stderr)
    
    return result.stdout.strip()

def main():
    load_data()

    db_url = 'postgresql+psycopg2://myuser:mypassword@host.docker.internal:5433/gtfs_db'
    engine = create_engine(db_url)

    src_id = "IDFM:472099" # input("Give the ID of your source stop: ")
    departure_date = 20260401
    dow = datetime.datetime(int(str(departure_date)[:4]), int(str(departure_date)[4:6]), int(str(departure_date)[6:])).weekday()
    departure_time = datetime.timedelta(hours=5, minutes=15)

    with engine.connect() as conn:
        conn.execute(text("CREATE TEMP TABLE input(source_id VARCHAR, departure_date INTEGER, day_of_week INTEGER, departure_time INTERVAL);"))
        conn.execute(text("INSERT INTO input (source_id, departure_date, day_of_week, departure_time) VALUES (:src, :departure_date, :day_of_week, :departure_time)"), {"src": src_id, "departure_date": departure_date, "day_of_week": dow, "departure_time": departure_time})
        conn.commit()

    sql_query = compile_logica('Fastest', 'logica/logica.l')
    
    # Separate Logica's PostgreSQL setup code from the actual SELECT statement
    # Logica's setup always ends with 'END $$;'
    logica_setup_separator = 'END $$;'
    parts = sql_query.rsplit(logica_setup_separator, 1)
    if len(parts) == 2:
        setup_sql = parts[0] + logica_setup_separator
        select_sql = parts[1].strip()
    else:
        setup_sql = ""
        select_sql = sql_query

    print("Executing query on PostgreSQL...")
    
    with engine.begin() as conn:
        if setup_sql:
            conn.execute(text(setup_sql))
            
        df = pd.read_sql(text(select_sql), conn)
        
    print(f"\nSuccessfully loaded {len(df)} edges! Head:")
    print(df.head())

if __name__ == "__main__":
    main()
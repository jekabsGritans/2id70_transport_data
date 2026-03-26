import subprocess

import pandas as pd
import os
from time import sleep
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
        capture_output=True, text=True, check=True
    )
    
    return result.stdout.strip()

def main():
    load_data()

    sql_query = compile_logica('Even', 'logica/logica.l')
    
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
    db_url = 'postgresql+psycopg2://myuser:mypassword@host.docker.internal:5433/gtfs_db'
    engine = create_engine(db_url)
    
    with engine.begin() as conn:
        if setup_sql:
            conn.execute(text(setup_sql))
            
        df = pd.read_sql(text(select_sql), conn)
        
    print(f"\nSuccessfully loaded {len(df)} edges! Head:")
    print(df.head())

if __name__ == "__main__":
    main()
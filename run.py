import subprocess

import pandas as pd
from sqlalchemy import create_engine, text


def get_logica_sql(predicate_name, file_path):
    print(f"Compiling {predicate_name} from {file_path}...")
    
    # Run Logica to compile the file
    result = subprocess.run(
        ['python3', '-m', 'logica', file_path, 'print', predicate_name],
        capture_output=True, text=True, check=True
    )
    
    return result.stdout.strip()

def main():
    # 1. Compile the SQL
    sql_query = get_logica_sql('Edges', 'edges.l')
    
    # 2. Separate Logica's PostgreSQL setup code from the actual SELECT statement
    # Logica's setup always ends with 'END $$;'
    parts = sql_query.rsplit('END $$;', 1)
    if len(parts) == 2:
        setup_sql = parts[0] + 'END $$;'
        select_sql = parts[1].strip()
    else:
        setup_sql = ""
        select_sql = sql_query

    # 3. Connect to PostgreSQL
    print("Executing query on PostgreSQL...")
    db_url = 'postgresql+psycopg2://myuser:mypassword@localhost:5432/gtfs_db'
    engine = create_engine(db_url)
    
    # 4. Execute the setup and then load the Edges
    with engine.begin() as conn:
        if setup_sql:
            conn.execute(text(setup_sql)) # Run the Logica environment setup
            
        df = pd.read_sql(text(select_sql), conn) # Fetch the actual data
        
    print(f"\nSuccessfully loaded {len(df)} edges!")
    print(df.head())

if __name__ == "__main__":
    main()
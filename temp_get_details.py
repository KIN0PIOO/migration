import oracledb
import os
from dotenv import load_dotenv

load_dotenv()

def get_details():
    lib_dir = os.getenv('ORACLE_CLIENT_PATH')
    oracledb.init_oracle_client(lib_dir=lib_dir)
    dsn = f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_SID')}"
    conn = oracledb.connect(user=os.getenv('DB_USER') or "scott", password=os.getenv('DB_PASS') or "tiger", dsn=dsn)
    cursor = conn.cursor()
    
    for table in ['EMPLOYEES', 'DEPARTMENTS', 'LOCATIONS', 'JOBS']:
        print(f"\n--- HR.{table} ---")
        cursor.execute(f"SELECT column_name, data_type, data_length FROM all_tab_columns WHERE owner = 'HR' AND table_name = '{table}' ORDER BY column_id")
        for c in cursor.fetchall():
            print(f"{c[0]}: {c[1]}({c[2]})")
            
    conn.close()

if __name__ == "__main__":
    get_details()

import sys, os
sys.path.append(os.getcwd())
from app.core.db import get_connection

def check_counts():
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        cur.execute("SELECT COUNT(*) FROM HR.EMPLOYEES")
        emp_count = cur.fetchone()[0]
        print(f"HR.EMPLOYEES: {emp_count}")
        
        for table in ['TGT_FAIL_ONCE', 'TGT_FAIL_TWICE', 'TGT_FAIL_ALWAYS']:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                count = cur.fetchone()[0]
                print(f"{table}: {count}")
            except Exception as e:
                print(f"{table}: TABLE NOT EXIST or ERROR ({e})")
        
        conn.close()
    except Exception as e:
        print(f"Error checking counts: {e}")

if __name__ == "__main__":
    check_counts()

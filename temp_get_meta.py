import oracledb
import os
from dotenv import load_dotenv

load_dotenv()

def get_meta():
    lib_dir = os.getenv('ORACLE_CLIENT_PATH')
    try:
        oracledb.init_oracle_client(lib_dir=lib_dir)
    except Exception as e:
        print(f"Oracle Client Init Warning: {e}")
        
    dsn = f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_SID')}"
    user = os.getenv('DB_USER') or "scott"
    password = os.getenv('DB_PASS') or "tiger"
    
    print(f"Connecting to {dsn} as {user}...")
    conn = oracledb.connect(user=user, password=password, dsn=dsn)
    cursor = conn.cursor()
    cursor = conn.cursor()
    
    # 접근 가능한 스키마 목록 확인
    print("--- Accessible Schemas ---")
    cursor.execute("SELECT username FROM all_users ORDER BY username")
    for u in cursor.fetchall():
        print(u[0])

    # HR 스키마의 테이블 목록 조회
    print("\n--- HR Tables ---")
    cursor.execute("SELECT table_name FROM all_tables WHERE owner = 'HR' AND table_name NOT LIKE 'BIN$%'")
    tables = [t[0] for t in cursor.fetchall()]
    print(f"Tables: {tables}")
    
    # 각 테이블의 컬럼 수 확인하여 복잡한 것 선택
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM all_tab_columns WHERE owner = 'HR' AND table_name = '{table}'")
        count = cursor.fetchone()[0]
        print(f"Table {table}: {count} columns")
        
    conn.close()

if __name__ == "__main__":
    get_meta()

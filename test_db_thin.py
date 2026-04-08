import oracledb
import os
from dotenv import load_dotenv

load_dotenv()

def test_db_thin():
    print("--- Oracle Thin 모드 접속 테스트 시작 ---")
    user = os.getenv("DB_USER") or "scott"
    password = os.getenv("DB_PASS") or "tiger"
    host = os.getenv("DB_HOST") or "localhost"
    port = os.getenv("DB_PORT") or "1521"
    sid = os.getenv("DB_SID") or "xe"
    
    dsn = f"{host}:{port}/{sid}"
    
    try:
        # Thin 모드로 접속 시도 (init_oracle_client 호출 없음)
        conn = oracledb.connect(user=user, password=password, dsn=dsn)
        print(f"접속 성공! (Server Version: {conn.version})")
        conn.close()
    except Exception as e:
        print(f"접속 실패: {e}")

if __name__ == "__main__":
    test_db_thin()

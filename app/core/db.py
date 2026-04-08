import oracledb
import os
from app.core.logger import logger
from dotenv import load_dotenv

# .env 로드는 시도하되, 실패해도 아래 기본값이 적용되도록 함
load_dotenv()

# Oracle DB 접속 정보 (빈 문자열이 와도 기본값을 사용하도록 'or' 연산자 적용)
DB_USER = os.getenv("DB_USER") or "scott"
DB_PASS = os.getenv("DB_PASS") or "tiger"
DB_HOST = os.getenv("DB_HOST") or "localhost"
DB_PORT = os.getenv("DB_PORT") or "1521"
DB_SID = os.getenv("DB_SID") or "xe"

# Oracle 11g support requires Thick mode
ORACLE_CLIENT_PATH = os.getenv("ORACLE_CLIENT_PATH") or r"C:\oraclexe\app\oracle\product\11.2.0\server\bin"

def get_connection():
    """Oracle DB에 접속하여 Connection 객체를 반환합니다."""
    try:
        # Thick 모드 초기화 (이미 된 경우 ProgrammingError 발생)
        try:
            oracledb.init_oracle_client(lib_dir=ORACLE_CLIENT_PATH)
        except oracledb.ProgrammingError:
            pass
            
        dsn = f"{DB_HOST}:{DB_PORT}/{DB_SID}"
        connection = oracledb.connect(
            user=DB_USER,
            password=DB_PASS,
            dsn=dsn
        )
        return connection
    except Exception as e:
        logger.error(f"[DB] Oracle 접속 중 에러 발생 (USER: {DB_USER}): {e}")
        raise e

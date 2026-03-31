import sqlite3
import os
from app.core.logger import logger

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "migration.db")

def get_connection():
    """SQLite DB에 접속하여 Connection 객체를 반환합니다."""
    try:
        connection = sqlite3.connect(DB_PATH)
        # 딕셔너리처럼 컬럼명으로 접근 가능하게 설정 (옵션)
        # connection.row_factory = sqlite3.Row
        return connection
    except Exception as e:
        logger.error(f"[DB] SQLite 접속 중 에러 발생: {e}")
        raise e

from app.core.exceptions import DBSqlError
from app.core.db import get_connection
from app.core.logger import logger
import sqlite3

def execute_migration(sql: str):
    """생성된 SQL을 SQLite DB 엔진에 실행"""
    logger.debug(f"[Executor] 실제 쿼리 실행 시작: {sql[:50]}...")
    
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            
            # DDL (CREATE TABLE) 과 DML (INSERT) 여러 줄을 한 번에 실행하기 위해 executescript 활용
            clean_sql = sql.strip()
            cursor.executescript(clean_sql)
            conn.commit()
            
            # executescript는 rowcount를 반환하지 않으므로 로그 메세지 단순화 확인
            logger.debug(f"[Executor] 지능형 구조 생성 및 마이그레이션 실행 성공")
            
    except Exception as e:
        logger.error(f"[Executor] 마이그레이션 쿼리 실패: {str(e)}")
        raise DBSqlError(f"SQLite 쿼리 실행 에러: {str(e)}")

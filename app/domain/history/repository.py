from app.core.logger import logger
from app.core.db import get_connection

def log_generated_sql(map_id: int, migration_sql: str, verification_sql: str):
    """(비즈니스 로그) LLM이 생성한 쿼리를 DB에 저장합니다."""
    logger.info(f"[HistoryRepo] map_id={map_id} | 마이그레이션 SQL DB 기록 진행")
    logger.debug(f"[HistoryRepo] Migration: {migration_sql[:50]}...")
    logger.debug(f"[HistoryRepo] Verification: {verification_sql[:50]}...")
    
    query = """
        UPDATE MAPPING_RULES
        SET MIG_SQL = ?, VERIFY_SQL1 = ?, UPD_DATE = CURRENT_TIMESTAMP
        WHERE MAP_ID = ?
    """
    
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (migration_sql, verification_sql, map_id))
            conn.commit()
    except Exception as e:
        logger.error(f"[HistoryRepo] SQL 생성 내역 기록 중 오류: {e}")

def log_business_history(map_id: int, status: str, message: str, retry_count: int = 0):
    """(비즈니스 로그) 각 실행 상태와 디테일 메시지(에러 등)를 저장합니다."""
    msg_str = str(message)
    if len(msg_str) > 4000:
        msg_str = msg_str[:3996] + "..."
        
    logger.info(f"[HistoryRepo] map_id={map_id} | Business Log 저장 -> [{status}] (Retry: {retry_count}) : {msg_str[:50]}")
    
    formatted_msg = f"[{status}] (Retry: {retry_count}) {msg_str}"
    
    query = """
        UPDATE MAPPING_RULES
        SET LOG = CASE 
            WHEN LOG IS NULL OR LOG = '' THEN ?
            ELSE LOG || CHAR(10) || ? 
        END, 
        UPD_DATE = CURRENT_TIMESTAMP
        WHERE MAP_ID = ?
    """
    
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (formatted_msg, formatted_msg, map_id))
            conn.commit()
    except Exception as e:
        logger.error(f"[HistoryRepo] 비즈니스 이력 기록 중 오류: {e}")

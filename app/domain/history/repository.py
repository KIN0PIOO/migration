from app.core.logger import logger

def log_generated_sql(map_id: int, migration_sql: str, verification_sql: str):
    """(비즈니스 로그) LLM이 생성한 쿼리를 DB에 저장합니다."""
    logger.info(f"[HistoryRepo] map_id={map_id} | 마이그레이션 SQL DB 기록 완료")
    logger.debug(f"[HistoryRepo] Migration: {migration_sql[:50]}...")
    logger.debug(f"[HistoryRepo] Verification: {verification_sql[:50]}...")
    
def log_business_history(map_id: int, status: str, message: str, retry_count: int = 0):
    """(비즈니스 로그) 각 실행 상태와 디테일 메시지(에러 등)를 저장합니다."""
    logger.info(f"[HistoryRepo] map_id={map_id} | Business Log 저장 -> [{status}] (Retry: {retry_count}) : {message[:50]}")

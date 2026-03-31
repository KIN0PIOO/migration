from app.core.logger import logger
from app.domain.mapping.models import MappingRule
from app.core.db import get_connection

def get_pending_jobs() -> list[MappingRule]:
    """USE_YN='Y' 이고 TASK_TARGET='Y'인 작업을 EXE_ORDER 순으로 가져옵니다."""
    logger.debug("[Repository] DB에서 작업 대상을 스캔합니다...")
    jobs = []
    
    query = """
        SELECT MAP_ID, MAP_TYPE, FROM_TABLE, TO_TABLE, FROM_COLUMNS, 
               TO_COLUMNS, USE_YN, TASK_TARGET, EXE_ORDER, 
               MIG_SQL, VERIFY_SQL1, VERIFY_SQL2, STATUS, LOG, 
               UPD_DATE, CORRECT_SQL, USER_EDITED
        FROM MAPPING_RULES
        WHERE USE_YN = 'Y' 
          AND TASK_TARGET = 'Y' 
          AND (STATUS IS NULL OR STATUS = 'PENDING')
        ORDER BY EXE_ORDER ASC
    """
    
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            
            for row in rows:
                rule = MappingRule(
                    map_id=row[0],
                    map_type=row[1],
                    from_table=row[2],
                    to_table=row[3],
                    from_columns=row[4],
                    to_columns=row[5],
                    use_yn=row[6],
                    task_target=row[7],
                    exe_order=row[8],
                    mig_sql=row[9],
                    verify_sql1=row[10],
                    verify_sql2=row[11],
                    status=row[12],
                    log=row[13],
                    upd_date=row[14],
                    correct_sql=row[15],
                    user_edited=row[16]
                )
                jobs.append(rule)
                
    except Exception as e:
        logger.error(f"[Repository] 작업 대상을 조회하는 중 오류 발생: {e}")
        
    return jobs

def lock_job(map_id: int) -> bool:
    """동시성 제어를 위해 해당 Job의 상태를 즉시 'P'(진행중) 등으로 바꿉니다."""
    logger.debug(f"[Repository] map_id={map_id} 작업을 선점(Lock)합니다. 상태 -> RUNNING")
    
    query = """
        UPDATE MAPPING_RULES 
        SET STATUS = 'RUNNING', UPD_DATE = CURRENT_TIMESTAMP
        WHERE MAP_ID = ? AND (STATUS IS NULL OR STATUS = 'PENDING')
    """
    
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (map_id,))
            if cursor.rowcount > 0:
                conn.commit()
                return True
            return False
    except Exception as e:
        logger.error(f"[Repository] 작업 선점 중 오류 발생 map_id={map_id}: {e}")
        return False

def update_job_status(map_id: int, status: str):
    """작업 성공/실패 시 상태값을 변경합니다."""
    logger.info(f"[Repository] map_id={map_id} | DB 상태를 {status} 로 업데이트합니다.")
    
    query = """
        UPDATE MAPPING_RULES 
        SET STATUS = ?, UPD_DATE = CURRENT_TIMESTAMP
        WHERE MAP_ID = ?
    """
    
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (status, map_id))
            conn.commit()
    except Exception as e:
        logger.error(f"[Repository] 작업 상태 업데이트 중 오류 발생 map_id={map_id}: {e}")

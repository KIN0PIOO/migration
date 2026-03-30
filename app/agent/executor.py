from app.core.exceptions import DBSqlError
import random

def execute_migration(sql: str):
    """생성된 SQL을 DB 엔진에 실행 (Mock)"""
    
    # 임의로 타임아웃, 문법 오류등 발생 현상 시뮬레이션 (30% 확률)
    if random.random() < 0.3:
        raise DBSqlError(f"ORA-00942: table or view does not exist on query: {sql[:20]}...")
        
    # 정상 실행 가정
    pass

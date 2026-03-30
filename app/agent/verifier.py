import random

def execute_verification(sql: str) -> tuple[bool, str]:
    """양 DB의 정합성을 대조하는 검증 SQL 실행 (Mock)"""
    
    # 임의로 데이터 불일치(AS-IS 10건, TO-BE 9건 등) 발생 현상 시뮬레이션 (20% 확률)
    if random.random() < 0.2:
        return False, "Data count mismatch: AS-IS 100, TO-BE 99 rows."
        
    return True, "All Verification Passed"

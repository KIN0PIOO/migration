from app.core.logger import logger
from app.domain.mapping.models import MappingRule

# 임시 메모리 DB 역할 (테스트용 데이터)
_MOCK_MAPPING_RULES = [
    MappingRule(1, "legacy_users", "new_users", "user_id, user_nm", "id, name", 1, "Y", "Y"),
    MappingRule(2, "legacy_orders", "new_orders", "ord_id, ord_amt", "id, amount", 2, "Y", "Y"),
]

def get_pending_jobs() -> list[MappingRule]:
    """WORK_YN='Y' 이고 USE_YN='Y'인 작업을 EXEC_ORDER 순으로 가져옵니다."""
    logger.debug("[Repository] DB에서 작업 대상을 스캔합니다...")
    jobs = [rule for rule in _MOCK_MAPPING_RULES if rule.work_yn == 'Y' and rule.use_yn == 'Y']
    # 순서 정렬
    jobs.sort(key=lambda x: x.exec_order)
    return jobs

def lock_job(map_id: int) -> bool:
    """동시성 제어를 위해 해당 Job의 상태를 즉시 'P'(진행중) 등으로 바꿉니다."""
    logger.debug(f"[Repository] map_id={map_id} 작업을 선점(Lock)합니다. 상태 -> P")
    for rule in _MOCK_MAPPING_RULES:
        if rule.map_id == map_id and rule.work_yn == 'Y':
            rule.work_yn = 'P' # Processing 으로 변경
            return True
    return False

def update_job_status(map_id: int, status: str):
    """작업 성공/실패 시 상태값을 변경합니다."""
    logger.info(f"[Repository] map_id={map_id} | DB 상태를 {status} 로 업데이트합니다.")
    for rule in _MOCK_MAPPING_RULES:
        if rule.map_id == map_id:
            if status == "SUCCESS" or status == "FAIL":
                rule.work_yn = 'N'  # 종료
            elif status == "RETRY":
                rule.work_yn = 'P'  # 아직 진행중
            elif status == "RUNNING":
                rule.work_yn = 'P'
            break

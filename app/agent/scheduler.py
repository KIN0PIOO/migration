from app.core.logger import logger
from app.agent.orchestrator import MigrationOrchestrator
from app.domain.mapping.repository import get_pending_jobs, lock_job
import traceback

orchestrator = MigrationOrchestrator()

def poll_database():
    """
    APScheduler에 의해 지정된 주기마다 호출되는 단일 스캔 폴백 함수입니다.
    DB 내 대기 중인 작업(PENDING)을 찾아 순차적으로 즉시 처리합니다.
    """
    try:
        logger.info("\n--- [Scheduler] DB 작업 대상 스캔 ---")
        
        jobs = get_pending_jobs()
        
        if not jobs:
            logger.info("현재 대기 중인 작업 대상(PENDING) 없음")
            return

        logger.info(f"처리 대상 작업 발견: {len(jobs)}건")
        
        for job in jobs:
            if lock_job(job.map_id): 
                orchestrator.process_job(job)
                
    except Exception as e:
        logger.error(f"[Scheduler] 시스템 에러 발생: {str(e)}")
        logger.error(traceback.format_exc())

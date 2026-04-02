from app.core.logger import logger
from app.agent.orchestrator import MigrationOrchestrator
from app.domain.mapping.repository import get_pending_jobs, lock_job
from app.core.exceptions import BatchAbortError
import traceback

orchestrator = MigrationOrchestrator()

def poll_database():
    try:
        logger.info("\n--- [Scheduler] DB 작업 대상 스캔 ---")
        
        jobs = get_pending_jobs()
        
        if not jobs:
            logger.info("현재 대기 중인 작업 대상 없음")
            return

        logger.info(f"처리 대상 작업 발견: {len(jobs)}건")
        
        for job in jobs:
            if lock_job(job.map_id):
                try:
                    orchestrator.process_job(job)
                except BatchAbortError as abort_err:
                    logger.critical(f"[BATCH_ABORT] 스케줄러가 배치를 심각한 오류로 조기 중단합니다: {abort_err}")
                    break
                
    except Exception as e:
        logger.error(f"[Scheduler] 시스템 에러 발생: {str(e)}")
        logger.error(traceback.format_exc())

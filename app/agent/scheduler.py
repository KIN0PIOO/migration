import time
from app.core.logger import logger
from app.agent.orchestrator import MigrationOrchestrator
from app.domain.mapping.repository import get_pending_jobs, lock_job

orchestrator = MigrationOrchestrator()

def run_scheduler():
    logger.info("Starting Data Migration Agent Scheduler (Mock Version)...")
    logger.info("주기적으로 DB의 대상 작업을 스캔합니다.")
    
    # 테스트 시뮬레이션을 위해 3번의 루프만 돌도록 제한합니다.
    for i in range(3):
        try:
            logger.info(f"\n--- [Scheduler] Polling Cycle {i+1} 시작 ---")
            
            # 1. 작업 대상 가져오기 (WORK_YN='Y' 인 것들)
            jobs = get_pending_jobs()
            
            if not jobs:
                logger.info("현재 대기 중인 작업이 없습니다.")
            else:
                logger.info(f"처리 대상 작업 발견: {len(jobs)}건")
                
                for job in jobs:
                    # 2. DB 업데이트를 통한 작업 선점 (상태값을 P로)
                    # 이를 통해 여러 대의 에이전트가 겹쳐서 실행되는 것을 방지합니다.
                    if lock_job(job.map_id): 
                        # 3. 오케스트레이터로 실제 파이프라인 수행
                        orchestrator.process_job(job)
                        
            # 가짜 대기 시간 (원래라면 10초 ~ 1분 단위)
            time.sleep(3)
            
        except Exception as e:
            logger.error(f"[Scheduler] 시스템 에러 발생, 잠시 대기 후 루프 재개: {str(e)}")
            time.sleep(3)
            
    logger.info("\n==========================================")
    logger.info("스케줄러 테스트 사이클(3회) 종료. 에이전트를 안전하게 종료합니다.")

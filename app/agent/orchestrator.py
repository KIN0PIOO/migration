import time
from app.core.logger import logger
from app.core.exceptions import LLMBaseError, DBSqlError, VerificationFailError, BatchAbortError
from app.agent.llm_client import generate_sqls
from app.agent.executor import execute_migration
from app.agent.verifier import execute_verification
from app.domain.mapping.repository import update_job_status
from app.domain.history.repository import log_generated_sql, log_business_history

class MigrationOrchestrator:
    def process_job(self, mapping_rule):
        logger.info(f"\n==========================================")
        logger.info(f"[JOB_START] 대상 작업(map_id={mapping_rule.map_id}) 프로세스 시작")
        
        llm_retry_count = 0
        db_retry_count = 0
        max_retries = 3
        last_error = None
        
        while True:
            try:
                # 1. SQL 생성 (피드백 반영)
                logger.debug(f"[STEP_START] map_id={mapping_rule.map_id} | 1. LLM 쿼리 생성 요청")
                migration_sql, verification_sql = generate_sqls(mapping_rule, last_error)
                log_generated_sql(mapping_rule.map_id, migration_sql, verification_sql)
                
                # 2. 마이그레이션 실행
                logger.debug(f"[STEP_START] map_id={mapping_rule.map_id} | 2. 쿼리 파싱 및 DB 실행")
                execute_migration(migration_sql)
                
                # 검증 단계 생략
                
                # 검증 단계 제외: 실행 무사고 시 바로 PASS 처리
                update_job_status(mapping_rule.map_id, "PASS")
                log_business_history(mapping_rule.map_id, "JOB_PASS", "Migration Executed Successfully")
                logger.info(f"[JOB_PASS] map_id={mapping_rule.map_id} | >>> 마이그레이션 통과 (검증 생략) <<<")
                return

            except LLMBaseError as e:
                llm_retry_count += 1
                logger.warning(f"[LLM_RETRY] map_id={mapping_rule.map_id} | retry={llm_retry_count} | error={e.__class__.__name__} | {str(e)}")
                if llm_retry_count <= max_retries:
                    time.sleep(1)  # Backoff
                else:
                    logger.error(f"[BATCH_ABORT] map_id={mapping_rule.map_id} | >>> LLM 예외 최대 재시도({max_retries}회) 초과. 배치 즉시 중단 <<<")
                    update_job_status(mapping_rule.map_id, "FAIL")
                    log_business_history(mapping_rule.map_id, "BATCH_ABORT", f"LLM 쳐대 예외 초과: {str(e)}", llm_retry_count)
                    raise BatchAbortError("LLM 계열 에러 지속 발생으로 인한 배치 중단") from e

            except (DBSqlError, VerificationFailError) as e:
                db_retry_count += 1
                logger.error(f"[BUSINESS_RETRY] map_id={mapping_rule.map_id} | retry={db_retry_count} | error={e.__class__.__name__} | {str(e)}")
                log_business_history(mapping_rule.map_id, "BUSINESS_RETRY", str(e), db_retry_count)
                
                last_error = str(e)
                if db_retry_count <= max_retries:
                    time.sleep(1)
                else:
                    logger.error(f"[JOB_FAIL] map_id={mapping_rule.map_id} | >>> 비즈니스 계열 최대 재시도({max_retries}회) 초과. 해당 작업 FAIL 판정 <<<")
                    update_job_status(mapping_rule.map_id, "FAIL")
                    log_business_history(mapping_rule.map_id, "JOB_FAIL", "최대 재시도 횟수 초과", db_retry_count)
                    return

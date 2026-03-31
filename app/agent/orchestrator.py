import time
from app.core.logger import logger
from app.core.exceptions import LLMRateLimitError, DBSqlError, VerificationFailError
from app.agent.llm_client import generate_sqls
from app.agent.executor import execute_migration
from app.agent.verifier import execute_verification
from app.domain.mapping.repository import update_job_status
from app.domain.history.repository import log_generated_sql, log_business_history

class MigrationOrchestrator:
    def process_job(self, mapping_rule):
        logger.info(f"\n==========================================")
        logger.info(f"[Orchestrator] 대상 작업(map_id={mapping_rule.map_id}) 프로세스 시작")
        
        retry_count = 0
        max_retries = 3
        last_error = None
        
        while retry_count <= max_retries:
            try:
                # 1. SQL 생성 (피드백 반영)
                logger.debug(f"[Orchestrator] map_id={mapping_rule.map_id} | 1. LLM 쿼리 생성 요청 (수행 횟수: {retry_count}회)")
                migration_sql, verification_sql = generate_sqls(mapping_rule, last_error)
                log_generated_sql(mapping_rule.map_id, migration_sql, verification_sql)
                
                # 2. 마이그레이션 실행
                logger.debug(f"[Orchestrator] map_id={mapping_rule.map_id} | 2. 쿼리 파싱 및 DB 실행")
                execute_migration(migration_sql)
                
                # 3. 데이터 검증 (사용자 요청에 의해 임시 제외됨)
                # logger.debug(f"[Orchestrator] map_id={mapping_rule.map_id} | 3. 양 DB 정합성 쿼리(Verification) 실행")
                # is_valid, verify_msg = execute_verification(verification_sql)
                
                # 검증 단계 제외: 실행 무사고 시 바로 성공 처리
                update_job_status(mapping_rule.map_id, "SUCCESS")
                log_business_history(mapping_rule.map_id, "SUCCESS", "Migration Executed Successfully (Verification Skipped)")
                logger.info(f"[Orchestrator] map_id={mapping_rule.map_id} | >>> 마이그레이션 성공 (검증 생략) <<<")
                return

            except LLMRateLimitError as e:
                # API 제한은 재시도 백오프를 길게 줌
                logger.warning(f"[Orchestrator] map_id={mapping_rule.map_id} | Rate Limit 초과 (Backoff 시뮬레이션)")
                time.sleep(1)  # Backoff
                retry_count += 1
                last_error = str(e)

            except (DBSqlError, VerificationFailError) as e:
                # 실행 실패 시 에러 사유를 LLM에게 다시 넣어주기 위해 last_error 세팅
                logger.error(f"[Orchestrator] map_id={mapping_rule.map_id} | >> 실행 중 DB 에러 발생 (사유: {str(e)})")
                log_business_history(mapping_rule.map_id, "RETRY", str(e), retry_count)
                
                retry_count += 1
                last_error = str(e)
                time.sleep(1)
                
        # 재시도 루프 다 소진 시 최종 실패 상태로 변경 ('Human in the loop' 대상)
        logger.error(f"[Orchestrator] map_id={mapping_rule.map_id} | >>> 최대 재시도({max_retries}회) 초과. 최종 FAIL 판정 <<<")
        update_job_status(mapping_rule.map_id, "FAIL")
        log_business_history(mapping_rule.map_id, "FAIL", "최대 재시도 횟수 초과", retry_count)

from app.core.exceptions import LLMRateLimitError, LLMConnectionError, LLMTokenLimitError
import random

def generate_sqls(mapping_rule, last_error=None):
    """
    LLM API를 호출하여 Migration SQL과 Verification SQL을 생성 (Mock)
    """
    # 20퍼 확률로 에러나게
    rand_val = random.random()
    if rand_val < 0.10:
        raise LLMRateLimitError("OpenAI API limit exceeded (HTTP 429).")
    elif rand_val < 0.15:
        raise LLMConnectionError("Timeout: Failed to connect to LLM endpoint.")
    elif rand_val < 0.20:
        raise LLMTokenLimitError("Token boundary exceeded for this model.")
        
    if mapping_rule.correct_sql:
        # 사용자가 CORRECT_SQL을 제공한 경우, Mock LLM은 이를 바로 정답으로 생성했다고 시뮬레이션 합니다.
        migration_sql = mapping_rule.correct_sql
    else:
        error_prompt = ""
        if last_error:
            error_prompt = f"\n-- [이전 에러 피드백 로그 반영]\n-- 에러내용: {last_error}"

        # 타겟 컬럼들 추출 (공백 제거 후 모두 TEXT 타입화)
        columns_def = [col.strip() + " TEXT" for col in mapping_rule.to_columns.split(",")]
        
        # 1. 대상 테이블이 없다면 자동으로 뼈대부터 만들어내는 보호 구문 (DDL)
        create_table_ddl = f"CREATE TABLE IF NOT EXISTS {mapping_rule.to_table} (\n    " + ",\n    ".join(columns_def) + "\n);"
        
        # 2. 실제 데이터 이관 구문 (DML)
        insert_dml = f"INSERT INTO {mapping_rule.to_table} ({mapping_rule.to_columns}) SELECT {mapping_rule.from_columns} FROM {mapping_rule.from_table};"
        
        # 3. LLM 쿼리 응답(멀티 스크립트)으로 직렬화
        migration_sql = f"{create_table_ddl}\n{insert_dml}\n{error_prompt}".strip()
    
    verification_sql = f"SELECT COUNT(*) FROM {mapping_rule.to_table} t1 FULL JOIN {mapping_rule.from_table} t2 ON ... WHERE mismatch"
    
    return migration_sql, verification_sql

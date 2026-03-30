from app.core.exceptions import LLMRateLimitError
import random

def generate_sqls(mapping_rule, last_error=None):
    """
    LLM API를 호출하여 Migration SQL과 Verification SQL을 생성 (Mock)
    """
    # 임의로 RateLimit 에러 발생 현상 시뮬레이션 (20% 확률)
    if random.random() < 0.2:
        raise LLMRateLimitError("OpenAI API limit exceeded (HTTP 429).")
        
    error_prompt = ""
    if last_error:
        error_prompt = f"\n[이전 에러 피드백 로그 반영]\n에러내용: {last_error}"

    # 임의의 프롬프트 조합 결과라고 가정
    migration_sql = f"INSERT INTO {mapping_rule.to_table} ({mapping_rule.to_columns}) SELECT {mapping_rule.from_columns} FROM {mapping_rule.from_table}; {error_prompt}"
    verification_sql = f"SELECT COUNT(*) FROM {mapping_rule.to_table} t1 FULL JOIN {mapping_rule.from_table} t2 ON ... WHERE mismatch"
    
    return migration_sql, verification_sql

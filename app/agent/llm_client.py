import os
import json
import google.generativeai as genai
from google.api_core import exceptions as genai_exceptions
from app.core.exceptions import LLMRateLimitError, LLMConnectionError, LLMTokenLimitError, LLMAuthenticationError
from app.core.logger import logger
from dotenv import load_dotenv

# .env 로드 (루트 경로 기준)
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), ".env")
load_dotenv(env_path)

def get_model():
    """안전하게 Google Gemini 모델 클라이언트를 반환합니다."""
    # 사용자가 .env의 OPENAI_API_KEY 자리에 구글 키를 넣었으므로 그대로 읽음
    api_key = os.getenv("OPENAI_API_KEY") 
    
    if not api_key or "your_openai" in api_key:
        error_msg = f"API Key가 설정되지 않았습니다. (Path: {env_path})"
        logger.error(f"[LLM] {error_msg}")
        raise LLMAuthenticationError(error_msg)
    
    # 구글 API 키 형식 확인 (AIza로 시작하는지)
    if not api_key.startswith("AIza"):
        logger.warning("[LLM] 입력된 키가 Google API 키 형식이 아닌 것 같습니다. 확인이 필요합니다.")

    genai.configure(api_key=api_key)
    
    # JSON 모드 활성화를 위한 설정
    return genai.GenerativeModel(
        model_name='gemini-2.5-flash',
        generation_config={"response_mime_type": "application/json"}
    )

def generate_sqls(mapping_rule, last_error=None, last_sql=None):
    """
    Google Gemini API를 호출하여 Oracle 11g 마이그레이션 SQL과 검증 SQL을 생성합니다.
    """
    model = get_model()
    from_table = mapping_rule.from_table
    to_table = mapping_rule.to_table
    
    # 컬럼 정보 정리
    details = mapping_rule.details
    mapping_info = "\n".join([f"- {d.from_column} -> {d.to_column}" for d in details])
    
    # 프롬프트 구성 (Oracle 11g 전문가 페르소나 적용)
    prompt = f"""
    당신은 Oracle 11g 데이터 마이그레이션 전문가입니다. 
    제시된 매핑 규칙을 기반으로 마이그레이션 SQL과 정합성 검증 SQL을 JSON 형식으로 생성하십시오.

    [매핑 규칙]
    - 소스 테이블: {from_table}
    - 타겟 테이블: {to_table}
    - 컬럼 매핑 정보:
    {mapping_info}

    [필수 요구사항]
    1. Migration SQL: 
       - 타겟 테이블이 없으면 생성하는 DDL과 데이터를 옮기는 SELECT INSERT DML을 포함하십시오.
       - Oracle 11g 환경이므로 레거시 호환 문법을 사용하십시오.
       - 여러 SQL 문장이 포함될 경우 각 문장은 반드시 슬래시(/) 단독 라인으로 구분하십시오.
       
    2. Verification SQL (2단계 정합성 검증):
       - 1단계: 전체 결과 건수(Row Count) 비교.
       - 2단계: 매핑된 모든 컬럼에 대해 Null이 아닌(유효한) 데이터의 개수가 일치하는지 비교.
       - 모든 차이값의 절대값 합계를 구하여 단일 컬럼 'DIFF'로 반환하는 SQL을 작성하십시오.
       - 예시: SELECT ABS((소스전체)-(타겟전체)) + ABS((소스컬럼1건수)-(타겟컬럼1건수)) + ... AS DIFF FROM DUAL

    응답은 반드시 'migration_sql'과 'verification_sql' 키를 가진 JSON 객체여야 합니다.
    """
    
    if last_error:
        prompt += f"\n\n[이전 실행 실패 피드백]\n- 실패한 SQL: {last_sql}\n- 발생한 에러: {last_error}\n- 작업: 위 에러를 분석하여 올바르게 수정한 쿼리를 다시 생성하십시오."

    try:
        response = model.generate_content(prompt)
        result = json.loads(response.text)
        
        migration_sql = result.get("migration_sql", "")
        verification_sql = result.get("verification_sql", "")
        
        logger.info(f"[LLM] SQL 생성 완료 (Model: gemini-2.5-flash)")
        return migration_sql, verification_sql

    except genai_exceptions.InvalidArgument as e:
        logger.error(f"[LLM] 잘못된 인자 또는 API 키: {e}")
        raise LLMAuthenticationError(f"Google API 인증 실패: {str(e)}")
    except genai_exceptions.ResourceExhausted as e:
        logger.error(f"[LLM] 할당량 초과: {e}")
        raise LLMTokenLimitError(f"Google API 할당량 초과: {str(e)}")
    except Exception as e:
        logger.error(f"[LLM] Gemini API 호출 중 에러: {e}")
        raise LLMConnectionError(f"LLM 연결 실패: {str(e)}")

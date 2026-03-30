import sys
import os

# 모듈 경로 설정을 위해 실행위치/상위 디렉토리를 path에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.logger import logger
from app.agent.scheduler import run_scheduler

if __name__ == "__main__":
    logger.info("====================================")
    logger.info(" 데이터 마이그레이션 에이전트 초기화 ")
    logger.info("====================================")
    
    # 스케줄러 실행 (무한루프 진입점 대체)
    run_scheduler()

import os
from app.agent.llm_client import get_model
from app.core.logger import logger
from dotenv import load_dotenv

# .env 로드
load_dotenv()

def test_connection():
    print("--- Gemini 2.5 Pro 연결 테스트 시작 ---")
    try:
        model = get_model()
        prompt = "Hello, can you respond with a short JSON message like {'status': 'ok'}?"
        response = model.generate_content(prompt)
        print(f"응답 결과: {response.text}")
        print("연결 성공!")
    except Exception as e:
        print(f"연결 실패: {e}")

if __name__ == "__main__":
    test_connection()

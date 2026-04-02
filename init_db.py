import sys
import os
import sqlite3
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.logger import logger
from app.core.db import DB_PATH, get_connection
import traceback


def init_db():
    try:
        conn = get_connection()
        cursor = conn.cursor()

        logger.info("=== DB 초기화 시작 ===")

        # ----------------------------
        # 1. 기존 테이블 삭제
        # ----------------------------
        tables = ["MAPPING_RULES", "user_old", "user_new", "order_old", "order_new"]
        for table in tables:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")

        # ----------------------------
        # 2. MAPPING_RULES 생성
        # ----------------------------
        cursor.execute("""
        CREATE TABLE MAPPING_RULES (
            MAP_ID INTEGER PRIMARY KEY,
            MAP_TYPE TEXT,
            FROM_TABLE TEXT,
            TO_TABLE TEXT,
            FROM_COLUMNS TEXT,
            TO_COLUMNS TEXT,
            USE_YN TEXT,
            TASK_TARGET TEXT,
            EXE_ORDER INTEGER,
            MIG_SQL TEXT,
            VERIFY_SQL1 TEXT,
            VERIFY_SQL2 TEXT,
            STATUS TEXT,
            LOG TEXT,
            UPD_DATE TIMESTAMP,
            CORRECT_SQL TEXT,
            USER_EDITED TEXT,
            BATCH_COUNT INTEGER DEFAULT 0
        )
        """)

        # ----------------------------
        # 3. AS-IS / TO-BE 테이블 생성
        # ----------------------------
        cursor.execute("""
        CREATE TABLE user_old (
            id INTEGER,
            name TEXT,
            age INTEGER
        )
        """)

        cursor.execute("""
        CREATE TABLE user_new (
            user_id TEXT,
            username TEXT,
            age TEXT
        )
        """)

        cursor.execute("""
        CREATE TABLE order_old (
            id INTEGER,
            user_id INTEGER,
            amount INTEGER
        )
        """)

        cursor.execute("""
        CREATE TABLE order_new (
            order_id TEXT,
            user_id TEXT,
            amount TEXT
        )
        """)

        # ----------------------------
        # 4. AS-IS 데이터 삽입
        # ----------------------------
        cursor.executemany("""
        INSERT INTO user_old VALUES (?, ?, ?)
        """, [
            (1, "Alice", 25),
            (2, "Bob", None),       # NULL 테스트
            (3, "Charlie", 30)
        ])

        cursor.executemany("""
        INSERT INTO order_old VALUES (?, ?, ?)
        """, [
            (101, 1, 500),
            (102, 2, 1000),
            (103, 3, 1500)
        ])

        # ----------------------------
        # 5. MAPPING RULE INSERT
        # ----------------------------

        mapping_data = [

            # ✅ 정상 케이스
            (1, "SIMPLE", "user_old", "user_new",
             "id, name, age", "user_id, username, age",
             "Y", "Y", 1, None, None, None, None, None, datetime.now(), None, None, 0),

            # ❗ 컬럼 mismatch (존재하지 않는 컬럼)
            (2, "SIMPLE", "user_old", "user_new",
             "id, wrong_column", "user_id, username",
             "Y", "Y", 2, None, None, None, None, None, datetime.now(), None, None, 0),

            # ❗ 테이블 없음
            (3, "SIMPLE", "not_exist_table", "user_new",
             "id, name", "user_id, username",
             "Y", "Y", 3, None, None, None, None, None, datetime.now(), None, None, 0),

            # 🔁 재시도 케이스 (나중에 일부러 실패 유도)
            (4, "SIMPLE", "order_old", "order_new",
             "id, user_id, amount", "order_id, user_id, amount",
             "Y", "Y", 4, None, None, None, None, None, datetime.now(), None, None, 0),

            # ❌ USE_YN = N (스킵 테스트)
            (5, "SIMPLE", "user_old", "user_new",
             "id, name", "user_id, username",
             "N", "Y", 5, None, None, None, None, None, datetime.now(), None, None, 0),

            # ❌ 이미 SUCCESS 상태
            (6, "SIMPLE", "user_old", "user_new",
             "id, name", "user_id, username",
             "Y", "Y", 6, None, None, None, "SUCCESS", None, datetime.now(), None, None, 0),

            # ❌ PROCESSING 상태 (lock 테스트)
            (7, "SIMPLE", "user_old", "user_new",
             "id, name", "user_id, username",
             "Y", "Y", 7, None, None, None, "PROCESSING", None, datetime.now(), None, None, 0),

        ]

        cursor.executemany("""
        INSERT INTO MAPPING_RULES VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, mapping_data)

        conn.commit()

        logger.info("=== DB 초기화 완료 ===")

    except Exception as e:
        logger.error("DB 초기화 중 오류 발생")
        logger.error(str(e))
        logger.error(traceback.format_exc())

    finally:
        conn.close()


if __name__ == "__main__":
    init_db()
import oracledb
import sys
import os

# 모듈 경로 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.db import get_connection

def reset_sequences(cursor):
    """시퀀스를 삭제 후 재생성하여 1번부터 시작하게 함"""
    seqs = ['MAPPING_RULES_SEQ', 'MAPPING_RULE_DETAIL_SEQ', 'MIGRATION_LOG_SEQ']
    for seq in seqs:
        try:
            cursor.execute(f"DROP SEQUENCE {seq}")
            print(f"Dropped sequence {seq}")
        except oracledb.DatabaseError:
            pass
        cursor.execute(f"CREATE SEQUENCE {seq} START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE")
        print(f"Created sequence {seq} starting with 1")

def setup_cases():
    print("Setting up HR migration cases in Oracle (ID 1, 2, 3 Fixed)...")
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # 1. 시퀀스 초기화
        reset_sequences(cursor)

        # 2. 관련 데이터 완전 삭제 (MAPPING_RULES 삭제 시 상세 내역도 연쇄 삭제됨)
        print("Cleaning up existing data...")
        cursor.execute("DELETE FROM MIGRATION_LOG")
        cursor.execute("DELETE FROM MAPPING_RULE_DETAIL")
        cursor.execute("DELETE FROM MAPPING_RULES")
        
        # 타겟 테이블들도 초기화 (실험을 위해 매번 새로 생성되게 함)
        target_tables = ['TGT_EMPLOYEES', 'TGT_JOBS', 'TGT_EMP_JOB_JOIN']
        for table in target_tables:
            try:
                cursor.execute(f"DROP TABLE {table}")
                print(f"Dropped target table {table}")
            except oracledb.DatabaseError:
                pass

        # 3. CASE 1: EMPLOYEES 전체 이관 (SIMPLE)
        mid_var = cursor.var(oracledb.NUMBER)
        cursor.execute("""
            INSERT INTO MAPPING_RULES (MAP_TYPE, FROM_TABLE, TO_TABLE, USE_YN, TASK_TARGET, PRIORITY, STATUS)
            VALUES ('SIMPLE', 'HR.EMPLOYEES', 'TGT_EMPLOYEES', 'Y', 'Y', 1, '')
            RETURNING MAP_ID INTO :mid
        """, mid=mid_var)
        map_id_1 = int(mid_var.getvalue()[0])
        
        emp_cols = [
            (1, 'EMPLOYEE_ID', 'ID'), (2, 'FIRST_NAME', 'F_NAME'), (3, 'LAST_NAME', 'L_NAME'),
            (4, 'EMAIL', 'EMAIL'), (5, 'HIRE_DATE', 'HIRE_DT'), (6, 'JOB_ID', 'JOB_ID'),
            (7, 'SALARY', 'SALARY')
        ]
        for seq, f, t in emp_cols:
            cursor.execute("INSERT INTO MAPPING_RULE_DETAIL (MAP_ID, SEQ, FROM_COLUMN, TO_COLUMN) VALUES (:1, :2, :3, :4)", (map_id_1, seq, f, t))
        print(f"Case 1 (EMPLOYEES) added. MAP_ID: {map_id_1}")

        # 4. CASE 2: JOBS 전체 이관 (SIMPLE)
        mid_var = cursor.var(oracledb.NUMBER)
        cursor.execute("""
            INSERT INTO MAPPING_RULES (MAP_TYPE, FROM_TABLE, TO_TABLE, USE_YN, TASK_TARGET, PRIORITY, STATUS)
            VALUES ('SIMPLE', 'HR.JOBS', 'TGT_JOBS', 'Y', 'Y', 2, '')
            RETURNING MAP_ID INTO :mid
        """, mid=mid_var)
        map_id_2 = int(mid_var.getvalue()[0])
        
        job_cols = [
            (1, 'JOB_ID', 'JOB_ID'), (2, 'JOB_TITLE', 'TITLE'), (3, 'MIN_SALARY', 'MIN_SAL'), (4, 'MAX_SALARY', 'MAX_SAL')
        ]
        for seq, f, t in job_cols:
            cursor.execute("INSERT INTO MAPPING_RULE_DETAIL (MAP_ID, SEQ, FROM_COLUMN, TO_COLUMN) VALUES (:1, :2, :3, :4)", (map_id_2, seq, f, t))
        print(f"Case 2 (JOBS) added. MAP_ID: {map_id_2}")

        # 5. CASE 3: EMPLOYEES + JOBS 조인 이관 (COMPLEX)
        mid_var = cursor.var(oracledb.NUMBER)
        cursor.execute("""
            INSERT INTO MAPPING_RULES (MAP_TYPE, FROM_TABLE, TO_TABLE, USE_YN, TASK_TARGET, PRIORITY, STATUS)
            VALUES ('COMPLEX', 'HR.EMPLOYEES E JOIN HR.JOBS J ON E.JOB_ID = J.JOB_ID', 'TGT_EMP_JOB_JOIN', 'Y', 'Y', 3, '')
            RETURNING MAP_ID INTO :mid
        """, mid=mid_var)
        map_id_3 = int(mid_var.getvalue()[0])
        
        join_cols = [
            (1, 'E.EMPLOYEE_ID', 'EMP_ID'), 
            (2, "E.FIRST_NAME || ' ' || E.LAST_NAME", 'FULL_NAME'),
            (3, 'J.JOB_TITLE', 'JOB_TITLE'),
            (4, 'E.SALARY', 'EMP_SALARY')
        ]
        for seq, f, t in join_cols:
            cursor.execute("INSERT INTO MAPPING_RULE_DETAIL (MAP_ID, SEQ, FROM_COLUMN, TO_COLUMN) VALUES (:1, :2, :3, :4)", (map_id_3, seq, f, t))
        print(f"Case 3 (JOIN) added. MAP_ID: {map_id_3}")

        # --- 심화 스트레스 테스트 시나리오 추가 ---
        
        # 6. CASE 4: 1회 실패 후 성공 (FAIL_ONCE)
        mid_var = cursor.var(oracledb.NUMBER)
        cursor.execute("""
            INSERT INTO MAPPING_RULES (MAP_TYPE, FROM_TABLE, TO_TABLE, USE_YN, TASK_TARGET, PRIORITY, STATUS)
            VALUES ('SIMPLE', 'HR.FAIL_ONCE', 'TGT_FAIL_ONCE', 'Y', 'Y', 4, '')
            RETURNING MAP_ID INTO :mid
        """, mid=mid_var)
        map_id_4 = int(mid_var.getvalue()[0])
        for seq, f, t in emp_cols[:3]: # ID, F_NAME, L_NAME 사용
            cursor.execute("INSERT INTO MAPPING_RULE_DETAIL (MAP_ID, SEQ, FROM_COLUMN, TO_COLUMN) VALUES (:1, :2, :3, :4)", (map_id_4, seq, f, t))
        print("Case 4 (Fail Once) added with details.")

        # 7. CASE 5: 2회 실패 후 성공 (FAIL_TWICE)
        mid_var = cursor.var(oracledb.NUMBER)
        cursor.execute("""
            INSERT INTO MAPPING_RULES (MAP_TYPE, FROM_TABLE, TO_TABLE, USE_YN, TASK_TARGET, PRIORITY, STATUS)
            VALUES ('SIMPLE', 'HR.FAIL_TWICE', 'TGT_FAIL_TWICE', 'Y', 'Y', 5, '')
            RETURNING MAP_ID INTO :mid
        """, mid=mid_var)
        map_id_5 = int(mid_var.getvalue()[0])
        for seq, f, t in emp_cols[:3]:
            cursor.execute("INSERT INTO MAPPING_RULE_DETAIL (MAP_ID, SEQ, FROM_COLUMN, TO_COLUMN) VALUES (:1, :2, :3, :4)", (map_id_5, seq, f, t))
        print("Case 5 (Fail Twice) added with details.")

        # 8. CASE 6: 3회 실패 시 최종 실패 (FAIL_ALWAYS)
        mid_var = cursor.var(oracledb.NUMBER)
        cursor.execute("""
            INSERT INTO MAPPING_RULES (MAP_TYPE, FROM_TABLE, TO_TABLE, USE_YN, TASK_TARGET, PRIORITY, STATUS)
            VALUES ('SIMPLE', 'HR.FAIL_ALWAYS', 'TGT_FAIL_ALWAYS', 'Y', 'Y', 6, '')
            RETURNING MAP_ID INTO :mid
        """, mid=mid_var)
        map_id_6 = int(mid_var.getvalue()[0])
        for seq, f, t in emp_cols[:3]:
            cursor.execute("INSERT INTO MAPPING_RULE_DETAIL (MAP_ID, SEQ, FROM_COLUMN, TO_COLUMN) VALUES (:1, :2, :3, :4)", (map_id_6, seq, f, t))
        print("Case 6 (Fail Always) added with details.")

        # 9. CASE 7: LLM 토큰 문제로 배치 총 중단 (BATCH_FAIL)
        mid_var = cursor.var(oracledb.NUMBER)
        cursor.execute("""
            INSERT INTO MAPPING_RULES (MAP_TYPE, FROM_TABLE, TO_TABLE, USE_YN, TASK_TARGET, PRIORITY, STATUS)
            VALUES ('SIMPLE', 'HR.BATCH_FAIL', 'TGT_BATCH_FAIL', 'Y', 'Y', 7, '')
            RETURNING MAP_ID INTO :mid
        """, mid=mid_var)
        map_id_7 = int(mid_var.getvalue()[0])
        for seq, f, t in emp_cols[:3]:
            cursor.execute("INSERT INTO MAPPING_RULE_DETAIL (MAP_ID, SEQ, FROM_COLUMN, TO_COLUMN) VALUES (:1, :2, :3, :4)", (map_id_7, seq, f, t))
        print("Case 7 (Batch Fail) added with details.")

        conn.commit()
        print("All stress test cases reset with valid column mappings.")
        
    except Exception as e:
        print(f"Error during setup_cases: {e}")
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    setup_cases()

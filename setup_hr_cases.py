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
    print("Setting up SCOTT migration cases (EMP, DEPT, SALGRADE)...")
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
        target_tables = [
            'TGT_EMP', 'TGT_DEPT', 'TGT_EMP_DEPT', 
            'TGT_FAIL_ONCE', 'TGT_FAIL_TWICE', 'TGT_FAIL_ALWAYS', 'TGT_BATCH_FAIL',
            'EMP_SAL_COMPLEX'
        ]
        for table in target_tables:
            try:
                cursor.execute(f"DROP TABLE {table}")
                print(f"Dropped target table {table}")
            except oracledb.DatabaseError:
                pass

        # --- CASE 1: EMP 전체 이관 (SIMPLE) ---
        mid_var = cursor.var(oracledb.NUMBER)
        cursor.execute("""
            INSERT INTO MAPPING_RULES (MAP_TYPE, FROM_TABLE, TO_TABLE, USE_YN, TASK_TARGET, PRIORITY, STATUS)
            VALUES ('SIMPLE', 'EMP', 'TGT_EMP', 'Y', 'Y', 1, '')
            RETURNING MAP_ID INTO :mid
        """, mid=mid_var)
        map_id_1 = int(mid_var.getvalue()[0])
        
        emp_cols = [
            (1, 'EMPNO', 'EMP_ID'), (2, 'ENAME', 'NAME'), (3, 'JOB', 'ROLE'),
            (4, 'MGR', 'MANAGER_ID'), (5, 'HIREDATE', 'JOIN_DATE'), (6, 'SAL', 'SALARY'),
            (7, 'COMM', 'COMMISSION'), (8, 'DEPTNO', 'DEPT_ID')
        ]
        for seq, f, t in emp_cols:
            cursor.execute("INSERT INTO MAPPING_RULE_DETAIL (MAP_ID, SEQ, FROM_COLUMN, TO_COLUMN) VALUES (:1, :2, :3, :4)", (map_id_1, seq, f, t))
        print(f"Case 1 (EMP) added. MAP_ID: {map_id_1}")

        # --- CASE 2: DEPT 전체 이관 (SIMPLE) ---
        mid_var = cursor.var(oracledb.NUMBER)
        cursor.execute("""
            INSERT INTO MAPPING_RULES (MAP_TYPE, FROM_TABLE, TO_TABLE, USE_YN, TASK_TARGET, PRIORITY, STATUS)
            VALUES ('SIMPLE', 'DEPT', 'TGT_DEPT', 'Y', 'Y', 2, '')
            RETURNING MAP_ID INTO :mid
        """, mid=mid_var)
        map_id_2 = int(mid_var.getvalue()[0])
        
        dept_cols = [
            (1, 'DEPTNO', 'DEPT_ID'), (2, 'DNAME', 'DEPT_NAME'), (3, 'LOC', 'LOCATION')
        ]
        for seq, f, t in dept_cols:
            cursor.execute("INSERT INTO MAPPING_RULE_DETAIL (MAP_ID, SEQ, FROM_COLUMN, TO_COLUMN) VALUES (:1, :2, :3, :4)", (map_id_2, seq, f, t))
        print(f"Case 2 (DEPT) added. MAP_ID: {map_id_2}")

        # --- CASE 3: EMP + DEPT 조인 이관 (COMPLEX) ---
        mid_var = cursor.var(oracledb.NUMBER)
        cursor.execute("""
            INSERT INTO MAPPING_RULES (MAP_TYPE, FROM_TABLE, TO_TABLE, USE_YN, TASK_TARGET, PRIORITY, STATUS)
            VALUES ('COMPLEX', 'EMP E JOIN DEPT D ON E.DEPTNO = D.DEPTNO', 'TGT_EMP_DEPT', 'Y', 'Y', 3, '')
            RETURNING MAP_ID INTO :mid
        """, mid=mid_var)
        map_id_3 = int(mid_var.getvalue()[0])
        
        join_cols = [
            (1, 'E.EMPNO', 'EMP_ID'), 
            (2, 'E.ENAME', 'EMP_NAME'),
            (3, 'D.DNAME', 'DEPT_NAME'),
            (4, 'E.SAL', 'SALARY')
        ]
        for seq, f, t in join_cols:
            cursor.execute("INSERT INTO MAPPING_RULE_DETAIL (MAP_ID, SEQ, FROM_COLUMN, TO_COLUMN) VALUES (:1, :2, :3, :4)", (map_id_3, seq, f, t))
        print(f"Case 3 (EMP-DEPT JOIN) added. MAP_ID: {map_id_3}")

        # --- 스트레스 테스트 시나리오 (EMP 기반) ---
        
        # 4. CASE 4: 1회 실패 후 성공 (FAIL_ONCE)
        mid_var = cursor.var(oracledb.NUMBER)
        cursor.execute("""
            INSERT INTO MAPPING_RULES (MAP_TYPE, FROM_TABLE, TO_TABLE, USE_YN, TASK_TARGET, PRIORITY, STATUS)
            VALUES ('SIMPLE', 'FAIL_ONCE_EMP', 'TGT_FAIL_ONCE', 'Y', 'Y', 4, '')
            RETURNING MAP_ID INTO :mid
        """, mid=mid_var)
        map_id_4 = int(mid_var.getvalue()[0])
        for seq, f, t in emp_cols[:3]:
            cursor.execute("INSERT INTO MAPPING_RULE_DETAIL (MAP_ID, SEQ, FROM_COLUMN, TO_COLUMN) VALUES (:1, :2, :3, :4)", (map_id_4, seq, f, t))
        print("Case 4 (Fail Once) added.")

        # 5. CASE 5: 2회 실패 후 성공 (FAIL_TWICE)
        mid_var = cursor.var(oracledb.NUMBER)
        cursor.execute("""
            INSERT INTO MAPPING_RULES (MAP_TYPE, FROM_TABLE, TO_TABLE, USE_YN, TASK_TARGET, PRIORITY, STATUS)
            VALUES ('SIMPLE', 'FAIL_TWICE_EMP', 'TGT_FAIL_TWICE', 'Y', 'Y', 5, '')
            RETURNING MAP_ID INTO :mid
        """, mid=mid_var)
        map_id_5 = int(mid_var.getvalue()[0])
        for seq, f, t in emp_cols[:3]:
            cursor.execute("INSERT INTO MAPPING_RULE_DETAIL (MAP_ID, SEQ, FROM_COLUMN, TO_COLUMN) VALUES (:1, :2, :3, :4)", (map_id_5, seq, f, t))
        print("Case 5 (Fail Twice) added.")

        # 6. CASE 6: 3회 실패 시 최종 실패 (FAIL_ALWAYS)
        mid_var = cursor.var(oracledb.NUMBER)
        cursor.execute("""
            INSERT INTO MAPPING_RULES (MAP_TYPE, FROM_TABLE, TO_TABLE, USE_YN, TASK_TARGET, PRIORITY, STATUS)
            VALUES ('SIMPLE', 'FAIL_ALWAYS_EMP', 'TGT_FAIL_ALWAYS', 'Y', 'Y', 6, '')
            RETURNING MAP_ID INTO :mid
        """, mid=mid_var)
        map_id_6 = int(mid_var.getvalue()[0])
        for seq, f, t in emp_cols[:3]:
            cursor.execute("INSERT INTO MAPPING_RULE_DETAIL (MAP_ID, SEQ, FROM_COLUMN, TO_COLUMN) VALUES (:1, :2, :3, :4)", (map_id_6, seq, f, t))
        print("Case 6 (Fail Always) added.")

        # 7. CASE 7: LLM 토큰 문제로 배치 총 중단 (BATCH_FAIL)
        mid_var = cursor.var(oracledb.NUMBER)
        cursor.execute("""
            INSERT INTO MAPPING_RULES (MAP_TYPE, FROM_TABLE, TO_TABLE, USE_YN, TASK_TARGET, PRIORITY, STATUS)
            VALUES ('SIMPLE', 'BATCH_FAIL_EMP', 'TGT_BATCH_FAIL', 'Y', 'Y', 7, '')
            RETURNING MAP_ID INTO :mid
        """, mid=mid_var)
        map_id_7 = int(mid_var.getvalue()[0])
        for seq, f, t in emp_cols[:3]:
            cursor.execute("INSERT INTO MAPPING_RULE_DETAIL (MAP_ID, SEQ, FROM_COLUMN, TO_COLUMN) VALUES (:1, :2, :3, :4)", (map_id_7, seq, f, t))
        print("Case 7 (Batch Fail) added.")

        # --- CASE 8: EMP + SALGRADE 복합 변환 (COMPLEX Example) ---
        mid_var = cursor.var(oracledb.NUMBER)
        cursor.execute("""
            INSERT INTO MAPPING_RULES (MAP_TYPE, FROM_TABLE, TO_TABLE, USE_YN, TASK_TARGET, PRIORITY, STATUS)
            VALUES ('COMPLEX', 'EMP E JOIN SALGRADE S ON E.SAL BETWEEN S.LOSAL AND S.HISAL', 'EMP_SAL_COMPLEX', 'Y', 'Y', 8, '')
            RETURNING MAP_ID INTO :mid
        """, mid=mid_var)
        map_id_8 = int(mid_var.getvalue()[0])
        
        real_complex_cols = [
            (1, 'E.EMPNO', 'EMP_ID'), 
            (2, 'E.ENAME', 'EMP_NAME'),
            (3, 'E.SAL', 'SALARY'),
            (4, 'S.GRADE', 'SALARY_GRADE'),
            (5, "CASE WHEN E.JOB = 'PRESIDENT' THEN 'VIP' WHEN E.JOB = 'MANAGER' THEN 'EXEC' ELSE 'STAFF' END", 'JOB_CATEGORY'),
            (6, "NVL(E.COMM, 0)", 'COMM_FIXED')
        ]
        for seq, f, t in real_complex_cols:
            cursor.execute("INSERT INTO MAPPING_RULE_DETAIL (MAP_ID, SEQ, FROM_COLUMN, TO_COLUMN) VALUES (:1, :2, :3, :4)", (map_id_8, seq, f, t))
        print(f"Case 8 (EMP-SALGRADE Complex) added. MAP_ID: {map_id_8}")

        conn.commit()
        print("All SCOTT-based test cases set successfully.")
        
    except Exception as e:
        print(f"Error during setup_cases: {e}")
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    setup_cases()

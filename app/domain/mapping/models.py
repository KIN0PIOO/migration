from dataclasses import dataclass
from typing import Optional
from datetime import datetime

@dataclass
class MappingRule:
    """DB에 저장된 메타데이터(Mapping Rule)를 표현하는 객체"""
    map_id: int
    map_type: str
    from_table: str
    to_table: str
    from_columns: str
    to_columns: str
    use_yn: str
    task_target: str       # 작업대상
    exe_order: int
    mig_sql: Optional[str] = None
    verify_sql1: Optional[str] = None
    verify_sql2: Optional[str] = None
    status: Optional[str] = None
    log: Optional[str] = None
    upd_date: Optional[datetime] = None
    correct_sql: Optional[str] = None
    user_edited: Optional[str] = None


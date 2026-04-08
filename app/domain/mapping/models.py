from dataclasses import dataclass, field
from typing import Optional, List
from datetime import datetime

@dataclass
class MappingDetail:
    """DB에 저장된 메타데이터(Mapping Rule Detail)를 표현하는 객체"""
    map_detail_id: int
    map_id: int
    seq: int
    from_column: str
    to_column: str

@dataclass
class MappingRule:
    """DB에 저장된 메타데이터(Mapping Rule)를 표현하는 객체"""
    map_id: int
    map_type: str
    from_table: str
    to_table: str
    use_yn: str
    task_target: str       # 작업대상
    priority: int          # 기존 exe_order
    mig_sql: Optional[str] = None
    verify_sql: Optional[str] = None
    status: Optional[str] = None
    correct_sql: Optional[str] = None
    user_edited: Optional[str] = None
    batch_count: int = 0
    elapsed_seconds: int = 0
    retry_count: int = 0
    verify_seq: int = 0
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    details: List[MappingDetail] = field(default_factory=list)

    @property
    def from_columns(self) -> str:
        """이관 대상의 from_column을 콤마(,)로 구분한 문자열 반환"""
        return ", ".join(d.from_column for d in sorted(self.details, key=lambda x: x.seq))

    @property
    def to_columns(self) -> str:
        """이관 대상의 to_column을 콤마(,)로 구분한 문자열 반환"""
        return ", ".join(d.to_column for d in sorted(self.details, key=lambda x: x.seq))


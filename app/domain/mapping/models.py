from dataclasses import dataclass

@dataclass
class MappingRule:
    """DB에 저장된 메타데이터(Mapping Rule)를 표현하는 객체"""
    map_id: int
    from_table: str
    to_table: str
    from_columns: str
    to_columns: str
    exec_order: int
    work_yn: str
    use_yn: str

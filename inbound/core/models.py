from enum import Enum
from typing import Any, DefaultDict, Dict, List, Optional, Protocol, Union

from pydantic import BaseModel, Field, create_model

from inbound.core.job_id import generate_id


class CsvWriter(Protocol):
    def writerows(self, rows):
        ...


class SyncMode(Enum):
    REPLACE = 1
    APPEND = 2


class Bookmark(BaseModel):
    column: str
    last_value: Any


class OracleSpec(BaseModel):
    pass


class BaseSpec(BaseModel):
    name: Optional[str] = None
    query: Optional[str] = None
    table: Optional[str] = "test"
    project_id: Optional[str] = None
    keyfile: Optional[str] = None
    chunksize: Optional[int] = 10000
    client_email: Optional[str] = None
    private_key: Optional[str] = None
    table_schema: Optional[List[Dict[str, str]]]
    database: Optional[str] = None
    database_schema: Optional[str] = Field(None, alias="schema")
    transformer: Optional[str] = None
    format: Optional[str] = None
    meta: DefaultDict[str, str] = None
    source: Optional[str] = None
    interface: Optional[str] = None
    row_id: Optional[Union[str, List]] = None
    profile: Optional[str] = None
    target: Optional[str] = None
    profiles_dir: Optional[str] = None


class SnowflakeSpec(BaseSpec):
    account: Optional[str] = None
    session_parameters: Optional[str] = None
    warehouse: Optional[str] = None


class SQLAlchemySpec(BaseSpec):
    connection_string: Optional[str] = None
    dsn: Optional[str] = None
    password: Optional[str] = None
    user: Optional[str] = None
    role: Optional[str] = None


class FileSpec(BaseSpec):
    path: Optional[str] = None
    url: Optional[str] = None
    type: str = "csv"
    sep: Optional[str] = ","
    encoding: Optional[str] = "utf-8"


class BucketSpec(BaseSpec):
    path: Optional[str] = None
    bucket: Optional[str] = None
    blob: Optional[str] = None
    prefix: Optional[str] = None
    format: Optional[str] = "parquet"


class Spec(SQLAlchemySpec, SnowflakeSpec, FileSpec, BucketSpec, OracleSpec):
    mode: Optional[str] = "append"
    pass


class Profile(BaseModel):
    name: Optional[str] = None
    type: Optional[str] = None
    spec: Spec = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.name is None and self.spec is not None:
            self.name = self.spec.name


class ProfileConfig(BaseModel):
    profile_name: str
    target: Optional[create_model("Target", target=(str, ...))] = None
    credentials: Optional[
        create_model("Credentials", credentials=(Dict[str, Any], ...))
    ] = None
    # target_name: str
    # threads: int


class ColumnModel(BaseModel):
    column_name: str
    data_type: str
    data_length: Optional[int]
    data_precision: Optional[int]
    nullable: str


class ACLModel(BaseModel):
    allowed: Optional[List[str]] = None
    denied: Optional[List[str]] = None


class JobModel(BaseModel):
    name: str
    job_id: Optional[str] = generate_id()
    acl: Optional[ACLModel] = None
    meta: Optional[Dict] = None
    source: Profile
    target: Profile


class JobsModel(BaseModel):
    jobs: List[JobModel]

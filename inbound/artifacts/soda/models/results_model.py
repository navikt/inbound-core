from typing import Any, List, Optional

from pydantic import BaseModel


class Metric(BaseModel):
    identity: str
    metricName: str
    value: int
    dataSourceName: str


class Location(BaseModel):
    filePath: str
    line: int
    col: int


class Check(BaseModel):
    identity: str
    name: str
    type: str
    definition: str
    resourceAttributes: List
    location: Location
    dataSource: str
    table: str
    filter: Any
    column: Any
    metrics: List[str]
    outcome: str
    outcomeReasons: List
    archetype: Any


class Log(BaseModel):
    level: str
    message: str
    timestamp: str
    index: int
    doc: Any
    location: Any


class Model(BaseModel):
    definitionName: Any
    defaultDataSource: str
    dataTimestamp: str
    scanStartTimestamp: str
    scanEndTimestamp: str
    hasErrors: bool
    hasWarnings: bool
    hasFailures: bool
    metrics: List[Metric]
    checks: List[Check]
    automatedMonitoringChecks: List
    profiling: List
    metadata: List
    logs: List[Log]

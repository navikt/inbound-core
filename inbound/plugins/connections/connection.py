import os
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Iterator, Protocol, Tuple, runtime_checkable

import pandas
from google.cloud.bigquery.table import Any

from inbound.core.job_result import JobResult
from inbound.core.logging import LOGGER
from inbound.core.models import Profile, Spec


class BaseConnection(ABC):
    @abstractmethod
    def __init__(self, profile: Profile, file: str):
        self.profile = profile
        name = Path(file).stem
        if self.profile.type is None:
            self.profile.type = name
        if self.profile.name is None:
            if self.profile.spec.name is not None:
                self.profile.name = self.profile.spec.name
            else:
                self.profile.name = name

        # set transformer if specified
        if self.profile.spec.transformer is not None:
            self.profile.spec.transformer = self._get_full_path(
                self.profile.spec.transformer
            )

        # set formatter if specified

    @abstractmethod
    def to_pandas(
        self, job_id: str = None
    ) -> Iterator[Tuple[pandas.DataFrame, JobResult]]:
        pass

    @abstractmethod
    def from_pandas(
        self, job_id: str = None, chunk_number: int = 0, mode: str = "append"
    ) -> Tuple[Any, JobResult]:
        pass

    @abstractmethod
    def drop(self) -> JobResult:
        pass

    def to_temp_file(self, format: str = "csv") -> Tuple[str, JobResult]:
        for df in self.to_pandas:
            temp_file_name = tempfile.mktemp()
            if format == "csv":
                df.to_csv(temp_file_name)
            else:
                df.to_parquet(temp_file_name)
            yield temp_file_name, JobResult(result="DONE", rows=len(df))

    @property
    def name(self) -> Spec:
        return self.profile.name or self.profile.type

    @property
    def type(self) -> Spec:
        return self.profile.type or self.profile.name

    def __str__(self) -> str:
        return self.profile.name or self.profile.type

    def _get_full_path(self, path: str) -> str:
        if path is not None:
            if Path(path).is_absolute():
                return path
            else:
                if os.getenv("INBOUND_DATA_PATH") is not None:
                    path = str(Path(os.getenv("INBOUND_DATA_PATH")) / path)
                else:
                    path = str(Path.cwd() / path)
        return path


class Connection(Protocol):
    def to_pandas(
        self, job_id: str = None
    ) -> Iterator[Tuple[pandas.DataFrame, JobResult]]:
        ...

    def from_pandas(
        self, job_id: str = None
    ) -> Iterator[Tuple[pandas.DataFrame, JobResult]]:
        ...

    def drop(self) -> JobResult:
        ...


@runtime_checkable
class ConnectionClass(Connection, Protocol):
    """
    This class is to check if connector instance is derived from connection
    """

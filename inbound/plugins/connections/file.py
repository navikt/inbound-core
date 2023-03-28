import datetime
import os
import tracemalloc
from pathlib import Path
from typing import Any, Iterator, Tuple

import openpyxl as xl
import pandas

from inbound.core import JobResult, Profile, connection_factory
from inbound.core.logging import LOGGER
from inbound.core.models import SyncMode
from inbound.plugins.connections.connection import BaseConnection


def _isExcel(path: str = None):
    if not path:
        return False
    try:
        _, file_extension = os.path.splitext(path)
        return file_extension in [".xls", ".xlsx"]
    except:
        return False


class FileConnection(BaseConnection):
    def __init__(self, profile: Profile):
        super().__init__(profile, __file__)

        self.profile = profile
        self.chunk_size = self.profile.spec.chunksize or 10000
        self.encoding = self.profile.spec.encoding or "utf-8"
        self.sep = self.profile.spec.sep or ";"
        self.sheet_name = self.profile.spec.sheet_name or 0
        self.header = self.profile.spec.header or 0

    def __enter__(self):
        if self.profile.spec.url is not None:
            self.path = self.profile.spec.url
            return self

        if self.profile.spec.path is not None:
            self.path = self._get_full_path(self.profile.spec.path)

        if self.profile.spec.transformer is not None:
            self.transform = self._get_full_path(self.profile.spec.transformer)

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def __str__(self) -> str:
        return self.name

    def to_pandas(
        self, job_id: str = None
    ) -> Iterator[Tuple[pandas.DataFrame, JobResult]]:

        job_res = JobResult(
            result="NO RUN",
            job_id=job_id,
            start_date_time=datetime.datetime.now(),
        )
        chunk_number = 0

        try:
            if _isExcel(self.path):
                df = pandas.read_excel(
                    self.path,
                    sheet_name=self.sheet_name,
                    header=self.header,
                )
                total_length = len(df)

                job_res.end_date_time = datetime.datetime.now()
                job_res.memory = tracemalloc.get_traced_memory()
                yield df, job_res
            else:  # default to csv
                file_reader = pandas.read_csv(
                    self.path,
                    sep=self.sep,
                    chunksize=self.chunk_size,
                    encoding=self.encoding,
                    index_col=False,
                )

                total_length = 0
                chunk_start_date_time = datetime.datetime.now()
                for chunk in file_reader:
                    job_res.result = "DONE"
                    job_res.start_date_time = chunk_start_date_time
                    job_res.end_date_time = datetime.datetime.now()
                    job_res.memory = tracemalloc.get_traced_memory()
                    job_res.chunk_number = chunk_number
                    job_res.task_name = f"Read chunk number {chunk_number} from file"
                    job_res.size = chunk.memory_usage(deep=True).sum()
                    job_res.rows = len(chunk)
                    chunk_number += 1
                    chunk_start_date_time = datetime.datetime.now()
                    yield chunk, job_res

        except Exception as e:
            job_res.result = "FAILED"
            job_res.start_date_time = chunk_start_date_time
            job_res.end_date_time = datetime.datetime.now()
            job_res.memory = tracemalloc.get_traced_memory()
            job_res.chunk_number = chunk_number
            job_res.task_name = f"Read from file"
            return pandas.DataFrame, job_res

    def from_pandas(
        self,
        df: pandas.DataFrame,
        job_id: str = None,
        chunk_number: int = 0,
        mode: str = "append",
    ) -> Tuple[Any, JobResult]:

        mode = (
            SyncMode.REPLACE
            if (chunk_number == 0 and mode == "replace")
            else SyncMode.APPEND
        )

        job_res = JobResult(
            result="NO RUN",
            job_id=job_id,
            start_date_time=datetime.datetime.now(),
            chunk_number=chunk_number,
            task_name=f"Persist chunk number {chunk_number}",
            size=df.memory_usage(deep=True).sum(),
            rows=len(df),
        )

        try:
            if mode == SyncMode.REPLACE:
                if _isExcel(self.path):
                    df.to_excel(self.path, sheet_name=self.sheet_name, index=False)
                else:
                    df.to_csv(
                        self.path,
                        sep=self.sep,
                        encoding=self.encoding,
                        index=False,
                    )
            else:  # append mode
                header = True
                mode = "w"
                row = 0
                if Path(self.path).is_file():
                    header = False
                    mode = "a"

                if _isExcel(self.path):
                    with pandas.ExcelWriter(
                        self.path, mode=mode, if_sheet_exists="overlay"
                    ) as writer:
                        df.to_excel(
                            writer,
                            index=False,
                            sheet_name=self.sheet_name,
                            header=header,
                        )
                else:
                    df.to_csv(
                        self.path,
                        sep=self.sep,
                        encoding=self.encoding,
                        mode="a",
                        header=header,
                        index=False,
                    )

            job_res.result = "DONE"
            job_res.memory = tracemalloc.get_traced_memory()
            job_res.end_date_time = datetime.datetime.now()
            return "DONE", job_res
        except Exception as e:
            job_res.result = "FAILED"
            job_res.memory = tracemalloc.get_traced_memory()
            job_res.end_date_time = datetime.datetime.now()
            return "FAILED", job_res

    def drop(self) -> JobResult():
        try:
            os.remove(self.path)
            return JobResult(result="DONE")
        except OSError:
            return JobResult(result="FAILED")


def register() -> None:
    """Register connector"""
    connection_factory.register("file", FileConnection)
    connection_factory.register("csv", FileConnection)
    connection_factory.register("excel", FileConnection)
    connection_factory.register("xls", FileConnection)
    connection_factory.register("xlsx", FileConnection)
    connection_factory.register("url", FileConnection)

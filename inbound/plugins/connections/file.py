import os
import time
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

        start_time = time.monotonic()

        if _isExcel(self.path):
            try:
                df = pandas.read_excel(
                    self.path,
                    sheet_name=self.sheet_name,
                    header=self.header,
                )
                total_length = len(df)
                batch_number = 0
                duration_seconds = time.monotonic() - start_time
                LOGGER.info(
                    f"Batch number {batch_number} of length {total_length} returned after {duration_seconds} nanoseconds"
                )
                yield df, JobResult(result="DONE")
            except Exception as e:
                LOGGER.error(
                    f"Error reading excel file {self.path}. {self.encoding}. {e}"
                )
                return [(pandas.DataFrame, JobResult())]
        else:  # default to csv
            try:
                file_reader = pandas.read_csv(
                    self.path,
                    sep=self.sep,
                    chunksize=self.chunk_size,
                    encoding=self.encoding,
                    index_col=False,
                )

                total_length = 0
                batch_number = 0
                for chunk in file_reader:
                    total_length += len(chunk)
                    batch_number += 1
                    duration_seconds = time.monotonic() - start_time
                    LOGGER.info(
                        f"Batch number {batch_number} of length {len(chunk)} returned after {duration_seconds} nanoseconds"
                    )
                    yield chunk, JobResult(result="DONE")
            except Exception as e:
                LOGGER.error(
                    f"Error reading csv file  {self.path}. {self.encoding} {e}"
                )
                return [(pandas.DataFrame, JobResult())]

    def from_pandas(
        self,
        df: pandas.DataFrame,
        job_id: str = None,
        chunk: int = 0,
        mode: str = "append",
    ) -> Tuple[Any, JobResult]:

        mode = (
            SyncMode.REPLACE if (chunk == 0 and mode == "replace") else SyncMode.APPEND
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
            return "DONE", JobResult(
                result="DONE", rows=len(df), size=df.memory_usage(index=True).sum()
            )
        except Exception as e:
            LOGGER.info(f"Error writing dataframe to file {self.path}. {str(e)}")
            return "FAILED", JobResult()

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

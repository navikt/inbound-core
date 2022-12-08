import os
import time
from pathlib import Path
from typing import Any, Iterator, Tuple

import pandas

from inbound.core import JobResult, Profile, connection_factory
from inbound.core.logging import LOGGER
from inbound.core.models import SyncMode
from inbound.plugins.connections.connection import BaseConnection


class FileConnection(BaseConnection):
    def __init__(self, profile: Profile):
        super().__init__(profile, __file__)

        self.profile = profile
        self.chunk_size = self.profile.spec.chunksize or 10000
        self.encoding = self.profile.spec.encoding or "utf-8"
        self.sep = self.profile.spec.sep or "\t"

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

        start_time = time.monotonic_ns()

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
                duration_ns = time.monotonic_ns() - start_time
                LOGGER.info(
                    f"Batch number {batch_number} of length {len(chunk)} returned after {duration_ns} nanoseconds"
                )
                yield chunk, JobResult(result="DONE")
        except Exception as e:
            LOGGER.error(f"Error reading csv  {self.path}. {e}")
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
                df.to_csv(self.path, sep=self.sep, encoding=self.encoding, index=False)
            else:
                if Path(self.path).is_file():
                    header = False
                else:
                    header = True

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
    connection_factory.register("url", FileConnection)

import datetime
import os
import shutil
import tempfile
import time
import tracemalloc
from pathlib import Path
from typing import Any, Iterator, Tuple

import duckdb
import pandas

from inbound.core import JobResult, Profile, connection_factory, logging
from inbound.core.models import SyncMode
from inbound.plugins.common import retry_with_backoff
from inbound.plugins.connections.connection import BaseConnection
from inbound.plugins.connections.gcs import GCSConnection

LOGGER = logging.LOGGER


class DuckDBConnection(BaseConnection):
    def __init__(self, profile: Profile):
        super().__init__(profile, __file__)

        self.database = profile.spec.database or ":memory:"
        self.connection = None
        self.bucket_name = profile.spec.bucket

    def __enter__(self):
        self.connection = self.get_connection()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.connection:
            self.connection.close()

    def __str__(self) -> str:
        return self.name

    @retry_with_backoff()
    def get_connection(self):
        if self.database == ":memory:" or Path(self.database).is_absolute():
            pass
        else:
            if os.getenv("INBOUND_DATA_PATH") is not None:
                self.database = str(
                    Path(os.getenv("INBOUND_DATA_PATH")) / self.database
                )
            else:
                self.database = str(Path.cwd() / self.database)

        try:
            conn = duckdb.connect(database=self.database)
            return conn
        except Exception:
            raise

    def to_pandas(
        self, job_id: str = None
    ) -> Iterator[Tuple[pandas.DataFrame, JobResult]]:
        query = self.profile.spec.query or f"SELECT * FROM {self.profile.spec.table}"
        chunk_size = self.profile.spec.chunksize
        df = pandas.DataFrame()

        job_res = JobResult(
            result="NOT RUN", job_id=job_id, task_name=f"duckbd to pandas"
        )

        if not query:
            raise ValueError("Please provide an SQL query string or table name.")

        chunk_number = 0
        total_rows = 0
        try:
            batch_reader = self.connection.execute(query).fetch_record_batch(
                chunk_size=chunk_size
            )
            while True:
                job_res.start_date_time = datetime.datetime.now()
                try:
                    df = batch_reader.read_next_batch().to_pandas()
                    row_count = len(df)
                    job_res.result = "DONE"
                    job_res.end_date_time = datetime.datetime.now()
                    job_res.memory = tracemalloc.get_traced_memory()
                    job_res.size = df.memory_usage(deep=True).sum()
                    job_res.rows = len(df)
                    job_res.chunk_number = chunk_number
                    chunk_number += 1
                    total_rows += row_count
                    yield df, job_res
                except StopIteration:
                    break
        except Exception as e:
            job_res.result = "FAILED"
            job_res.end_date_time = datetime.datetime.now()
            job_res.memory = tracemalloc.get_traced_memory()
            return None, job_res

    def to_dir(self, format: str = "csv") -> Tuple[str, JobResult]:
        query = self.profile.spec.query or f"SELECT * FROM {self.profile.spec.table}"

        temp_dir_name = tempfile.mkdtemp()

        file_name = (
            Path(temp_dir_name)
            / f"duckdb_{datetime.datetime.now().strftime('%Y_%m_%d_%s')}.{format}"
        )

        query = f"COPY ({query}) TO '{file_name}' (FORMAT '{format}');"

        if not query:
            raise ValueError("Please provide an SQL query string or table name.")

        try:
            LOGGER.info(f"Excuting query {query} in database {self.type}:{self.name}")

            res = self.connection.execute(query)
            LOGGER.info(
                f"Result of query {query} exported to file {file_name}. Result {res}"
            )
            return temp_dir_name, JobResult(result="DONE")
        except Exception as e:
            LOGGER.info(f"Error excuting query {query} in database {self.name}")
            if Path(temp_dir_name).exists():
                shutil.rmtree(temp_dir_name)
            return None, JobResult("FAILED")

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

        table = self.profile.spec.table

        job_res = JobResult(
            result="NOT RUN",
            job_id=job_id,
            task_name=f"pandas to duckbd",
            size=df.memory_usage(deep=True).sum(),
            rows=len(df),
            start_date_time=datetime.datetime.now(),
            chunk_number=chunk_number,
        )

        try:
            if mode == SyncMode.REPLACE:
                self.drop(table)
                self.connection.execute(f"CREATE TABLE {table} AS SELECT * from df")
            else:
                self.connection.execute(
                    f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * from df"
                )

            job_res.memory = tracemalloc.get_traced_memory()
            job_res.end_date_time = datetime.datetime.now()
            job_res.result = "DONE"
            return self.name, job_res
        except Exception as e:
            job_res.memory = tracemalloc.get_traced_memory()
            job_res.end_date_time = datetime.datetime.now()
            job_res.result = "FAILED"
            return self.name, job_res

    def from_parquet(self, file_name: str, mode: SyncMode) -> JobResult:
        table = self.profile.spec.table

        try:
            if mode == SyncMode.REPLACE:
                self.drop(table)
                self.connection.execute(
                    f"CREATE TABLE {table} AS SELECT * FROM read_parquet({file_name})"
                )
            else:
                self.connection.execute(
                    f"INSERT INTO {table} SELECT * FROM read_parquet({file_name})"
                )
            return JobResult(result="DONE")
        except Exception as e:
            LOGGER.info(
                f"Error writing parquet file {file_name} to table {table} in SQL database {self.name}. {str(e)}"
            )
            return JobResult()

    def drop(self, table_name: str) -> JobResult():
        try:
            self.connection.execute(f"DROP TABLE IF EXISTS {table_name}")
            return JobResult(result="DONE")
        except Exception as e:
            LOGGER.info(
                f"Database error: Could not drop table {table_name} in SQL database {self.name}. {str(e)}"
            )
            return JobResult()


def register() -> None:
    """Register connector"""
    connection_factory.register("duckdb", DuckDBConnection)

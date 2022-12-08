import datetime
import os
import shutil
import tempfile
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

        if not query:
            raise ValueError("Please provide an SQL query string or table name.")

        batch_number = 1
        total_rows = 0
        try:
            LOGGER.info(
                f"Excuting query {query} in database {self.type}:{self.name} with chunksize: {chunk_size}"
            )
            batch_reader = self.connection.execute(query).fetch_record_batch(
                chunk_size=chunk_size
            )
            while True:
                try:
                    chunk = batch_reader.read_next_batch()
                    length = len(chunk)
                    LOGGER.info(
                        f"Returning batch number {batch_number} of length {length} for chunk size: {chunk_size}"
                    )
                    batch_number += 1
                    total_rows += length
                    yield chunk.to_pandas(), JobResult(result="DONE")
                except StopIteration:
                    break
        except Exception as e:
            LOGGER.info(
                f"Error excuting query {query} in database {self.name} with chunksize: {chunk_size}"
            )
            return [], JobResult(result="DONE")

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
        chunk: int = 0,
        mode: str = "append",
    ) -> Tuple[Any, JobResult]:
        mode = (
            SyncMode.REPLACE if (chunk == 0 and mode == "replace") else SyncMode.APPEND
        )

        table = self.profile.spec.table

        try:
            if mode == SyncMode.REPLACE:
                self.drop(table)
                self.connection.execute(f"CREATE TABLE {table} AS SELECT * from df")
            else:
                self.connection.execute(
                    f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * from df"
                )
                self.connection.append(table_name=table, df=df)
            return "DONE", JobResult(
                result="DONE", rows=len(df), size=df.memory_usage().sum()
            )
        except Exception as e:
            LOGGER.info(
                f"Error writing dataframe to table {table} in SQL database {self.name}. {str(e)}"
            )
            return "FAILED", JobResult()

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

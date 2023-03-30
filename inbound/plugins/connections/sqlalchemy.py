import datetime
import tracemalloc
from typing import Any, Iterator, Tuple

import pandas
import sqlalchemy

from inbound.core import JobResult, Profile, connection_factory, logging
from inbound.core.models import SyncMode
from inbound.plugins.common import retry_with_backoff
from inbound.plugins.connections.connection import BaseConnection, Connection

LOGGER = logging.LOGGER


class SQLAlchemyConnection(BaseConnection):
    def __init__(self, profile: Profile):
        super().__init__(profile, __file__)

        self.engine = None
        self.connection = None

    def __enter__(self):
        conn_string = self.profile.spec.connection_string

        if not conn_string:
            raise ValueError("Please provide a connection string.")

        try:
            self.engine = sqlalchemy.create_engine(conn_string)
            self.connection = self.get_connection()
        except Exception as e:
            LOGGER.error(f"Error connecting to database. {e}")

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.connection:
            self.connection.close()

        if self.engine:
            LOGGER.info(f"SQLAlchemy engine dispose")
            self.engine.dispose()

    def __str__(self) -> str:
        return self.name

    @retry_with_backoff()
    def get_connection(self) -> Connection:
        try:
            LOGGER.info(f"SQLAlchemy connect")
            conn = self.engine.connect()
            return conn
        except Exception as e:
            LOGGER.error(f"Error in trying to connect to db")
            raise

    def to_pandas(
        self, job_id: str = None
    ) -> Iterator[Tuple[pandas.DataFrame, JobResult]]:
        query = self.profile.spec.query or f"SELECT * FROM {self.profile.spec.table}"
        chunk_size = self.profile.spec.chunksize or 100000

        if not query:
            raise ValueError("Please provide an SQL query string or table name.")

        LOGGER.info(
            f"Excuting query {query} in database {self.type}:{self.name} with chunksize: {chunk_size}"
        )

        job_res = JobResult(
            result="NO RUN",
            job_id=job_id,
            start_date_time=datetime.datetime.now(),
            task_name=f"chunk to pandas",
        )

        chunk_number = 0
        length = 1
        total_rows = 1
        chunk_start_date_time = datetime.datetime.now()
        df = pandas.DataFrame()

        try:
            iterator = pandas.read_sql(query, self.engine, chunksize=chunk_size)
            while True:
                try:
                    df = next(iterator)
                    LOGGER.info(
                        f"Returning batch number {chunk_number} of length {len(df)}"
                    )
                    job_res.result = ("DONE",)
                    job_res.start_date_time = chunk_start_date_time
                    job_res.end_date_time = datetime.datetime.now()
                    job_res.memory = tracemalloc.get_traced_memory()
                    job_res.chunk_number = chunk_number
                    job_res.size = df.memory_usage(deep=True).sum()
                    job_res.rows = len(df)
                    chunk_number += 1
                    chunk_start_date_time = datetime.datetime.now()
                    chunk_number += 1
                    total_rows += length
                    yield df, job_res
                except StopIteration:
                    LOGGER.info("Last chunk read")
                    break

        except Exception as e:
            LOGGER.error(f"Could not read from {query}")
            job_res.result = ("FAILED",)
            job_res.start_date_time = chunk_start_date_time
            job_res.end_date_time = datetime.datetime.now()
            job_res.memory = tracemalloc.get_traced_memory()
            job_res.chunk_number = chunk_number
            job_res.rows = len(df)
            return [], job_res

    def to_sql(
        self,
        df: pandas.DataFrame,
        table: str,
    ) -> None:
        df.to_sql(
            table, con=self.connection, index=False, if_exists="append", method="multi"
        )

    def from_pandas(
        self,
        df: pandas.DataFrame,
        chunk_number: int = 0,
        mode: str = "replace",
        job_id: str = None,
    ) -> Tuple[Any, JobResult]:

        sync_mode = (
            SyncMode.REPLACE
            if (chunk_number == 0 and mode == "replace")
            else SyncMode.APPEND
        )

        job_res = JobResult(
            result="NO RUN",
            job_id=job_id,
            task_name=f"from pandas",
            start_date_time=datetime.datetime.now(),
            chunk_number=chunk_number,
            size=df.memory_usage(deep=True).sum(),
            rows=len(df),
        )

        table = self.profile.spec.table

        try:
            if sync_mode == SyncMode.REPLACE:
                job_res = self.drop(table, job_res)
                job_res.log()

            self.to_sql(df, table)

            job_res.memory = tracemalloc.get_traced_memory()
            job_res.end_date_time = datetime.datetime.now()
            job_res.result = "DONE"
            return None, job_res

        except Exception as e:
            job_res.memory = tracemalloc.get_traced_memory()
            job_res.end_date_time = datetime.datetime.now()
            job_res.result = "FAILED"
            return None, job_res

    def execute(self, sql):
        return self.engine.execute(sql)

    def drop(self, table_name: str, job_res: JobResult = None) -> JobResult():

        if job_res is None:
            job_res = JobResult()

        job_res.start_date_time = datetime.datetime.now()
        job_res.task_name = f"drop table {table_name}"
        try:
            metadata = sqlalchemy.MetaData()
            table = sqlalchemy.Table(table_name, metadata)
            insp = sqlalchemy.inspect(self.engine)
            if table is not None:
                metadata.drop_all(self.engine, [table], checkfirst=True)

            job_res.end_date_time = datetime.datetime.now()
            job_res.memory = tracemalloc.get_traced_memory()
            job_res.result = "DONE"
            return job_res
        except Exception as e:
            LOGGER.info(
                f"Database error: Could not drop table {table_name} in SQL database {self.name}. {str(e)}"
            )
            job_res.result = "FAILED"
            return job_res


def register() -> None:
    """Register connector"""
    connection_factory.register("sqlalchemy", SQLAlchemyConnection)

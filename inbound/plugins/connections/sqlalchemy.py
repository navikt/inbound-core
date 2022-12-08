# from inbound.plugins.common import await_connection
import datetime
import os
import sys
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
            self.engine.dispose()

    def __str__(self) -> str:
        return self.name

    @retry_with_backoff()
    def get_connection(self) -> Connection:
        try:
            conn = self.engine.connect()
            return conn
        except Exception as e:
            LOGGER.error(f"Error in trying to connect to db")
            raise

    def to_pandas(
        self, job_id: str = None
    ) -> Iterator[Tuple[pandas.DataFrame, JobResult]]:
        query = self.profile.spec.query or f"SELECT * FROM {self.profile.spec.table}"
        chunk_size = self.profile.spec.chunksize or 1000

        if not query:
            raise ValueError("Please provide an SQL query string or table name.")

        LOGGER.info(
            f"Excuting query {query} in database {self.type}:{self.name} with chunksize: {chunk_size}"
        )

        try:
            iterator = pandas.read_sql(query, self.engine, chunksize=chunk_size)
            batch_number = 1
            length = 1
            total_rows = 1
            while True:
                try:
                    df = next(iterator)
                    LOGGER.info(
                        f"Returning batch number {batch_number} of length {len(df)}"
                    )
                    batch_number += 1
                    total_rows += length
                    yield df, JobResult(result="DONE", rows=len(df))
                except StopIteration:
                    break

        except Exception as e:
            LOGGER.error(f"Could not read from {query}")
            return [], JobResult()

    def from_pandas(
        self,
        df: pandas.DataFrame,
        job_id: str = None,
        table_name: str = None,
        chunk: int = 0,
        mode: str = "replace",
    ) -> Tuple[Any, JobResult]:

        mode = (
            SyncMode.REPLACE if (chunk == 0 and mode == "replace") else SyncMode.APPEND
        )

        table = self.profile.spec.table

        job_result = JobResult(job_id=job_id)

        try:
            if mode == SyncMode.REPLACE:
                self.drop(table)

            df.to_sql(table, con=self.connection, index=False, if_exists="append")

            job_result.rows = len(df)
            job_result.size = df.memory_usage().sum()
            job_result.end_date_time = datetime.datetime.now()
            job_result.result = "DONE"
            LOGGER.info(job_result)
            return "DONE", job_result

        except Exception as e:
            LOGGER.info(
                f"Error writing dataframe to table {table} in SQL database {self.name}. {str(e)}"
            )
            return "FAILED", JobResult()

    def execute(self, sql):
        self.engine.execute(sql)

    def drop(self, table_name: str) -> JobResult():
        try:
            metadata = sqlalchemy.MetaData()
            table = sqlalchemy.Table(table_name, metadata)
            insp = sqlalchemy.inspect(self.engine)
            if table is not None:
                metadata.drop_all(self.engine, [table], checkfirst=True)
            return JobResult(result="DONE")
        except Exception as e:
            LOGGER.info(
                f"Database error: Could not drop table {table_name} in SQL database {self.name}. {str(e)}"
            )
            return JobResult()


def register() -> None:
    """Register connector"""
    connection_factory.register("sqlalchemy", SQLAlchemyConnection)

import os
import tempfile
from typing import TextIO, Tuple

from inbound.core import JobResult, Profile, connection_factory, logging
from inbound.plugins.connections.sqlalchemy import SQLAlchemyConnection

LOGGER = logging.LOGGER


class PostgresConnection(SQLAlchemyConnection):
    def __init__(self, profile: Profile):
        super().__init__(profile)

    def copy_from(self, table) -> Tuple[TextIO, JobResult]:

        # Tip: These operations are not as efficient as the SQL COPY command with a file or program data source or destination,
        # because all data must pass through the client/server connection. For large amounts of data the SQL command might be preferable.

        try:
            with tempfile.NamedTemporaryFile(suffix=".csv") as tf:
                os.environ["PGPASSWORD"] = "postgres"
                res = os.system(
                    f"psql -h 0.0.0.0 -p 5432 -d postgres -U postgres -c '\copy {table} to {tf.name} csv header'"
                )
                LOGGER.info(f"Postgres table {table} copied to {tf.name}")
                return tf, JobResult(result="DONE")
        except Exception as e:
            LOGGER.error(f"Error copying Postgres table {table} to {tf.name}. {e}")

        return None, JobResult()


def register() -> None:
    """Register connector"""
    connection_factory.register("postgres", PostgresConnection)

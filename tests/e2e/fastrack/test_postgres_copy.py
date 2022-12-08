import os

import numpy as np

from inbound.core.models import Profile, Spec
from inbound.plugins.connections.postgres import PostgresConnection
from inbound.plugins.connections.snowflake import SnowflakeConnection
from inbound.plugins.utils import df

pg_spec = Spec(
    name="postgres localhost",
    connection_string="postgresql+psycopg2://postgres:postgres@localhost:5432/postgres",
    table="test_table",
    query="select * from test_table",
)
pg_profile = Profile(type="postgres", name="postgres localhost", spec=pg_spec)


def test_to_csv(table="city"):

    with PostgresConnection(profile=pg_profile) as db:
        file, job_result = db.copy_from(table)

        assert job_result.success

import os

import numpy as np

from inbound.core.models import Profile, Spec
from inbound.plugins.connections.postgres import PostgresConnection
from inbound.plugins.utils import df

spec = Spec(
    name="postgres localhost",
    connection_string="postgresql+psycopg2://postgres:postgres@localhost:5432/postgres",
    table="test_table",
    query="select * from test_table",
)
profile = Profile(type="postgres", name="postgres localhost", spec=spec)


def test_write_pandas_append():

    # split in 4 chunks
    chunks = np.array_split(df, 4)
    with PostgresConnection(profile=profile) as db:
        for index in range(len(chunks)):
            ret, job_res = db.from_pandas(chunks[index], chunk=index)

        assert job_res.success


def test_roundtrip():

    with PostgresConnection(profile=profile) as db:
        db.from_pandas(df)
        res = db.to_pandas()
        df_res, _ = next(res)

        assert df_res.size > 0


def test_pandas_replace():

    with PostgresConnection(profile=profile) as db:
        ret, job_res = db.from_pandas(df)

        assert job_res.success


def test_to_csv(table="city"):

    with PostgresConnection(profile=profile) as db:
        file, ret_res = db.copy_from(table)
        assert ret_res.success


def test_drop_table():

    with PostgresConnection(profile=profile) as db:
        db.from_pandas(df)
        ret = db.drop(profile.spec.table)
        assert ret.result == "DONE"


if __name__ == "__main__":
    os.environ["PGPASSWORD"] = "postgres"
    test_to_csv()

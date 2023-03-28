import os

import numpy as np

from inbound.core.models import Profile, Spec
from inbound.plugins.connections.snowflake import SnowflakeConnection
from inbound.plugins.utils import df

user = os.getenv("SNOWFLAKE_DATAPRODUCT_USER")
pwd = os.getenv("SNOWFLAKE_DATAPRODUCT_PASSWORD")
account = (
    os.getenv("SNOWFLAKE_DATAPRODUCT_ACCOUNT")
    + "."
    + os.getenv("SNOWFLAKE_DATAPRODUCT_REGION")
)

spec = Spec(
    name=f"snowflake {account}",
    connection_string=f"snowflake://{user}:{pwd}@{account}/vdl_dataproduct/dev?warehouse=vdl_dataproduct_warehouse",
    table="test_table",
    query="select * from test_table",
)

profile = Profile(type="snowflake", name=f"snowflake {account}", spec=spec)


def test_write_pandas_append():

    # split in 4 chunks
    chunks = np.array_split(df, 4)
    with SnowflakeConnection(profile=profile) as db:
        for index in range(len(chunks)):
            _, job_res = db.from_pandas(chunks[index], chunk_number=index)

        assert job_res.result == "DONE"


def test_roundtrip():

    with SnowflakeConnection(profile=profile) as db:
        db.from_pandas(df)
        res = db.to_pandas()
        df_res, _ = next(res)

        assert df_res.size > 0


def test_pandas_replace():

    with SnowflakeConnection(profile=profile) as db:
        res, job_res = db.from_pandas(df)
        assert job_res.result == "DONE"


def test_drop_table():

    with SnowflakeConnection(profile=profile) as db:
        db.from_pandas(df)
        res = db.drop(profile.spec.table)
        assert res.result == "DONE"

import numpy as np

from inbound.core.models import Profile, Spec
from inbound.plugins.connections.oracle import OracleConnection
from inbound.plugins.utils import df

basic_spec = Spec(
    name="oracle oracledb test",
    user="system",
    password="oracle",
    dsn="localhost:1521/?service_name=xe",
    table="test_table",
    query="select * from test_table",
)

oracle_profile = Profile(spec=basic_spec)

sa_spec = Spec(
    connection_string=f"oracle://system:oracle@localhost:1521/?service_name=xe",
    name="oracle alchemy test",
)

sa_profile = Profile(spec=sa_spec)


def test_write_pandas_append():
    # split in 4 chunks
    chunks = np.array_split(df, 4)
    with OracleConnection(profile=sa_profile) as db:
        for index in range(len(chunks)):
            ret, job_res = db.from_pandas(chunks[index], chunk=index)

        assert job_res.success


def test_roundtrip():
    with OracleConnection(profile=sa_profile) as db:
        db.from_pandas(df)
        res = db.to_pandas()
        df_res, _ = next(res)

        assert df_res.size > 0


def test_pandas_replace():
    with OracleConnection(profile=sa_profile) as db:
        ret, job_res = db.from_pandas(df)

        assert job_res.success


def test_drop_table():
    with OracleConnection(profile=sa_profile) as db:
        db.from_pandas(df)
        ret = db.drop(oracle_profile.spec.table)

        assert ret.result == "DONE"

import numpy as np

from inbound.core.models import Profile, Spec
from inbound.plugins.connections.duckdb import DuckDBConnection
from inbound.plugins.utils import df

credentials = Spec(name="duckdb", database=":memory", table="test")
profile = Profile(spec=credentials)


def test_write_pandas_append():

    # split in 4 chunks
    chunks = np.array_split(df, 4)
    with DuckDBConnection(profile=profile) as db:
        for index in range(len(chunks)):
            ret, job_res = db.from_pandas(chunks[index], chunk=index)

        assert job_res.result == "DONE"


def test_roundtrip():

    with DuckDBConnection(profile=profile) as db:
        db.from_pandas(df)
        res_iterator = db.to_pandas()
        df_res, job_res = next(res_iterator)

        assert df_res.size > 0


def test_pandas_replace():

    with DuckDBConnection(profile=profile) as db:
        ret, job_res = db.from_pandas(df)

        assert job_res.result == "DONE"


def test_drop_table():

    with DuckDBConnection(profile=profile) as db:
        db.from_pandas(df)
        ret = db.drop(profile.spec.table)

        assert ret.result == "DONE"

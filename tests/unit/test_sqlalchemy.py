import numpy as np

from inbound.core.models import Profile, Spec
from inbound.plugins.connections.sqlalchemy import SQLAlchemyConnection
from inbound.plugins.utils import df

spec = Spec(
    name="db",
    connection_string="sqlite://",
    table="test_table",
    query="select * from test_table",
)
profile = Profile(type="sqllite", name="sqlite memory", spec=spec)


def test_write_pandas_append():

    # split in 4 chunks
    chunks = np.array_split(df, 4)
    with SQLAlchemyConnection(profile=profile) as db:
        for index in range(len(chunks)):
            ret, job_res = db.from_pandas(chunks[index], chunk_number=index)

        assert job_res.result == "DONE"


def test_roundtrip():

    with SQLAlchemyConnection(profile=profile) as db:
        db.from_pandas(df)

        res_iterator = db.to_pandas()
        df_res, job_res = next(res_iterator)

        assert df_res.size > 0


def test_pandas_replace():

    with SQLAlchemyConnection(profile=profile) as db:
        res, job_res = db.from_pandas(df)

        assert job_res.result == "DONE"


def test_drop_table():

    with SQLAlchemyConnection(profile=profile) as db:
        db.from_pandas(df)
        ret = db.drop(profile.spec.table)

        assert ret.result == "DONE"


if __name__ == "__main__":
    test_pandas_replace()

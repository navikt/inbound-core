from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from inbound.core.models import Profile, Spec
from inbound.plugins.connections.file import FileConnection
from inbound.plugins.utils import df


@pytest.fixture(scope="function")
def profile(data_path):
    credentials = Spec(name="csv", path=data_path + "/test.csv")
    return Profile(spec=credentials)


def test_write_pandas_append(profile):

    # split in 4 chunks
    chunks = np.array_split(df, 4)
    with FileConnection(profile=profile) as db:
        for index in range(len(chunks)):
            res, job_res = db.from_pandas(chunks[index], chunk=index)

        assert job_res.result == "DONE"


def test_roundtrip(profile):

    with FileConnection(profile=profile) as db:
        db.from_pandas(df, mode="replace")
        df_out = pd.DataFrame()
        for df_res, job_res in db.to_pandas():
            if len(df_out) > 0:
                df_out = df_out.append(df_res)
            else:
                df_out = df_res

        assert len(df) == len(df_out)


def test_pandas_replace(profile):

    with FileConnection(profile=profile) as db:
        res, job_res = db.from_pandas(df)
        assert job_res.result == "DONE"


def test_drop(profile):

    with FileConnection(profile=profile) as db:
        db.from_pandas(df)
        ret = db.drop()

        assert ret.result == "DONE"

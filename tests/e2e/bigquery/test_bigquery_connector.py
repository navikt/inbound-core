import os

import pandas as pd

from inbound.core.models import Profile
from inbound.plugins.connections.bigquery import BigQueryConnection

from ....inbound.plugins.utils import df

KEYFILE = os.environ["TARGET_BIGQUERY_CREDENTIALS"]
KEYFILE_JSON = os.environ["TARGET_BIGQUERY_CREDENTIALS_JSON"]
PROJECT = os.environ["TARGET_BIGQUERY_PROJECT_ID"]
DATASET = os.environ["TARGET_BIGQUERY_DATASET"]
TYPE = "bigquery"


def test_pandas_to_bigquery_json_creds():

    config = {
        "type": TYPE,
        "keyfile": KEYFILE_JSON,
        "project_id": PROJECT,
        "table": f"{PROJECT}.{DATASET}.TEST",
    }

    sink = {"type": "bigquery", "name": "test_sink", "config": config}

    connector = Profile(**sink)

    with BigQueryConnection(profile=connector) as db:
        ret = db.from_pandas(df)

        assert ret.result == "DONE"


def test_pandas_to_bigquery():

    config = {
        "type": TYPE,
        "keyfile": KEYFILE,
        "project_id": PROJECT,
        "table": f"{PROJECT}.{DATASET}.TEST",
    }

    sink = {"type": "bigquery", "name": "test_sink", "config": config}

    connector = Profile(**sink)

    with BigQueryConnection(profile=connector) as db:
        ret = db.from_pandas(df)

        assert ret.result == "DONE"


def test_pandas_to_bigquery_append():

    config = {
        "type": TYPE,
        "keyfile": KEYFILE,
        "project_id": PROJECT,
        "table": f"{PROJECT}.{DATASET}.TEST",
    }

    sink = {"type": "bigquery", "name": "test_sink", "config": config}

    connector = Profile(**sink)

    with BigQueryConnection(profile=connector) as db:
        for index, row in df.iterrows():
            ret = db.from_pandas(df, index)

        assert ret.result == "DONE"


def test_roundtrip_to_bigquery():

    config = {
        "type": TYPE,
        "keyfile": KEYFILE,
        "project_id": PROJECT,
        "table": f"{PROJECT}.{DATASET}.TEST",
        "query": f"SELECT * FROM {PROJECT}.{DATASET}.TEST_NEW",
    }

    sink = {"type": "bigquery", "name": "test_sink", "config": config}

    connector = Profile(**sink)

    with BigQueryConnection(profile=connector) as db:
        res = db.from_pandas(df)

        res = db.to_pandas()
        df_re, _ = next(res)

        assert df_res.size > 0


def test_delete_bigquery_table():

    config = {
        "type": TYPE,
        "keyfile": KEYFILE,
        "project_id": PROJECT,
        "table": f"{PROJECT}.{DATASET}.TEST_NEW",
    }

    sink = {"type": "bigquery", "name": "test_sink", "config": config}

    connector = Profile(**sink)

    with BigQueryConnection(profile=connector) as db:
        db.from_pandas(df)
        ret = db.drop(f"{PROJECT}.{DATASET}.TEST_NEW")
        assert ret.result == "DONE"

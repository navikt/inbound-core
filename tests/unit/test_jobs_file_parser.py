import json
from typing import Dict, Union

import pytest

from inbound.core.jobs import _get_json_config
from inbound.core.logging import LOGGER
from inbound.core.models import JobsModel


@pytest.fixture
def get_spec_string():
    return '{"jobs": [{"name": "CSV to DuckDB", "source": {"type": "file", "spec": {"path": "source.csv"}}, "target": {"type": "duckdb", "spec": {"table": "test", "chunksize": 3, "database": "{{env_var(\'INBOUND_DATA_PATH\')}}/duckdb"}}}, {"name": "DuckDB to Parquet in GCS", "source": {"type": "duckdb", "spec": {"query": "select * from test", "database": "{{env_var(\'INBOUND_DATA_PATH\')}}/duckdb"}}, "target": {"type": "gcs", "spec": {"bucket": "sync-service", "blob": "test"}}}]}'


@pytest.fixture
def get_spec_json():
    return {
        "jobs": [
            {
                "name": "CSV to DuckDB",
                "source": {"type": "file", "spec": {"path": "source.csv"}},
                "target": {
                    "type": "duckdb",
                    "spec": {
                        "table": "test",
                        "chunksize": 3,
                        "database": "{{env_var('INBOUND_DATA_PATH')}}/duckdb",
                    },
                },
            },
            {
                "name": "DuckDB to Parquet in GCS",
                "source": {
                    "type": "duckdb",
                    "spec": {
                        "query": "select * from test",
                        "database": "{{env_var('INBOUND_DATA_PATH')}}/duckdb",
                    },
                },
                "target": {
                    "type": "gcs",
                    "spec": {"bucket": "sync-service", "blob": "test"},
                },
            },
        ]
    }


def test_yml_config(data_path: Union[str, Dict]):

    yml_file = data_path + "/csv_duckdb_gcs.yml"

    json_spec = _get_json_config(yml_file)

    spec = JobsModel(**json_spec)

    assert len(spec.jobs) > 0


def test_json_string_config(get_spec_string):

    json_spec = _get_json_config(get_spec_string)

    spec = JobsModel(**json_spec)

    assert len(spec.jobs) > 0


def test_json_dict_config(get_spec_json):

    json_spec = _get_json_config(get_spec_json)

    spec = JobsModel(**json_spec)

    assert len(spec.jobs) > 0


if __name__ == "__main__":
    import os

    from inbound.core.settings import Settings

    settings = Settings()
    os.environ["GOOGLE_CLOUD_PROJECT"] = settings.spec.gcp.project_id
    os.environ["INBOUND_DATA_PATH"] = "/Users/paulbencze/Projects/dbc/data"

    test_yml_config("/Users/paulbencze/Projects/dbc/data")

    def get_spec_string():
        return '{"jobs": [{"name": "CSV to DuckDB", "source": {"type": "file", "spec": {"path": "source.csv"}}, "target": {"type": "duckdb", "spec": {"table": "test", "chunksize": 3, "database": "{{env_var(\'INBOUND_DATA_PATH\')}}/duckdb"}}}, {"name": "DuckDB to Parquet in GCS", "source": {"type": "duckdb", "spec": {"query": "select * from test", "database": "{{env_var(\'INBOUND_DATA_PATH\')}}/duckdb"}}, "target": {"type": "gcs", "spec": {"bucket": "sync-service", "blob": "test"}}}]}'

    test_json_string_config(get_spec_string())

    def get_spec_json():
        return {
            "jobs": [
                {
                    "name": "CSV to DuckDB",
                    "source": {"type": "file", "spec": {"path": "source.csv"}},
                    "target": {
                        "type": "duckdb",
                        "spec": {
                            "table": "test",
                            "chunksize": 3,
                            "database": "{{env_var('INBOUND_DATA_PATH')}}/duckdb",
                        },
                    },
                },
                {
                    "name": "DuckDB to Parquet in GCS",
                    "source": {
                        "type": "duckdb",
                        "spec": {
                            "query": "select * from test",
                            "database": "{{env_var('INBOUND_DATA_PATH')}}/duckdb",
                        },
                    },
                    "target": {
                        "type": "gcs",
                        "spec": {"bucket": "sync-service", "blob": "test"},
                    },
                },
            ]
        }

    test_json_dict_config(get_spec_json())

import json

import yaml
from pydantic import ValidationError

from inbound.core.jobs import run_job
from inbound.core.logging import LOGGER
from inbound.core.models import JobsModel
from inbound.core.package import DATA_DIR


def test_validate_schema(data_path):
    with open(data_path + "/csv_duckdb_sqlite.yml", "r") as stream:
        try:
            jobs_spec = yaml.safe_load(stream)
        except yaml.YAMLError as e:
            LOGGER.info(f"Error loading jobs .yml file: {e}")
            assert False

    try:
        JobsModel.parse_obj(jobs_spec)
        assert True
    except ValidationError as e:
        LOGGER.info(f"Error parsing job: {e}")
        assert False


def test_csv_duckdb_job(data_path):
    ret = run_job(data_path + "/csv_duckdb_sqlite_csv.yml")

    assert ret.result == "DONE"

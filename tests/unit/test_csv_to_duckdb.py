import os

import yaml
from pydantic import ValidationError

from inbound.core.jobs import run_job
from inbound.core.logging import LOGGER
from inbound.core.models import JobsModel


def test_validate_schema(data_path):
    with open(data_path + "/csv_duckdb.yml", "r") as stream:
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
    ret = run_job(data_path + "/csv_duckdb.yml")

    assert ret.result == "DONE"


def test_csv_duckdb_job_with_transformer(data_path):
    ret = run_job(data_path + "/csv_duckdb_transformer.yml")

    assert ret.result == "DONE"


def test_csv_duckdb_job_with_metadata(data_path):
    ret = run_job(data_path + "/csv_duckdb_metadata.yml")

    assert ret.result == "DONE"

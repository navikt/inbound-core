import os

from pydantic import ValidationError

from inbound.core.jobs import run_job
from inbound.core.logging import LOGGER
from inbound.core.models import JobsModel
from inbound.core.package import DATA_DIR

os.environ["INBOUND_DATA_PATH"] = str(DATA_DIR)

job = {
    "jobs": [
        {
            "name": "CSV to DuckDB",
            "source": {
                "type": "file",
                "spec": {
                    "path": "{{env_var('INBOUND_DATA_PATH')}}/locations.csv",
                    "sep": "|",
                    "format": "log",
                    "source": "local file",
                    "interface": "csv",
                    "row_id": "id",
                    "mode": "replace",
                },
            },
            "target": {
                "type": "duckdb",
                "spec": {
                    "table": "test_meta",
                    "database": "{{env_var('INBOUND_DATA_PATH')}}/tempdb",
                },
            },
        }
    ]
}


def test_validate_schema():
    try:
        model = JobsModel.parse_obj(job)
        assert type(model) == JobsModel
    except ValidationError as e:
        print(f"Error parsing job: {e}")
        assert False


def test_csv_duckdb_job():
    ret = run_job(job)

    assert ret.result == "DONE"

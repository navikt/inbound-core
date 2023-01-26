import os
import shutil
import tempfile

import pytest

from inbound.core.package import DATA_DIR
from inbound.core.settings import Settings


@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    os.environ["INBOUND_DATA_PATH"] = str(DATA_DIR)
    os.environ["INBOUND_JOBS_PATH"] = str(DATA_DIR / "jobs")
    os.environ["DBT_PROFILES_DIR"] = str(DATA_DIR / "dbt")


@pytest.fixture(scope="function")
def data_path():
    return os.environ["INBOUND_DATA_PATH"]


def pytest_sessionstart(session):
    """
    Called after the Session object has been created and
    before performing collection and entering the run test loop.
    """

    settings = Settings()
    os.environ["GOOGLE_CLOUD_PROJECT"] = settings.spec.gcp.project_id

    secret_dir = tempfile.TemporaryDirectory()
    os.environ["INBOUND_SECRET_PATH"] = secret_dir.name

    os.environ["DBT_PROFILES_DIR"] = str(DATA_DIR / "dbt")


def pytest_sessionfinish(session, exitstatus):
    """
    Called after test run finished, right before
    returning the exit status to the system.
    """

    secret_dir = os.environ["INBOUND_SECRET_PATH"]

    os.environ.pop("DBT_PROFILES_DIR", None)

    try:
        shutil.rmtree(secret_dir)
    except:
        pass

import json
import os
from pathlib import Path
from unittest import mock

import pytest

import inbound.core.secrets as secrets
from inbound.core.logging import LOGGER
from inbound.core.settings import Settings


@pytest.fixture(autouse=True)
def mock_settings_env_vars(data_path):
    with mock.patch.dict(
        os.environ,
        {"INBOUND_SECRETS_PATH": data_path + "/vault"},
    ):
        yield


def test_single_env():
    LOGGER.info(f'secrets path: {os.getenv("INBOUND_SECRETS_PATH")}')
    secrets.set_env_variables_from_secrets(Settings())
    assert os.getenv("TEST_SECRET") == "123"


def test_single_json_env():
    LOGGER.info(f'secrets path: {os.getenv("INBOUND_SECRETS_PATH")}')
    secrets.set_env_variables_from_secrets(Settings())
    with open(os.getenv("TEST_SECRET_JSON")) as f:
        secret_json = json.load(f)
        assert secret_json["secret"] == "123"

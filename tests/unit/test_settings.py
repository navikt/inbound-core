import os

from inbound.core.settings import Settings


def test_settings_spec():
    current_env = os.getenv("INBOUND_VERSION")
    if os.getenv("INBOUND_VERSION") is not None:
        os.environ.pop("INBOUND_VERSION")
    set = Settings()
    assert set.version == "0.0.1"
    if current_env is not None:
        os.environ["INBOUND_VERSION"] = current_env


def test_settings_env():
    current_env = os.getenv("INBOUND_VERSION")
    os.environ["INBOUND_VERSION"] = "0.0.3"
    set = Settings()
    assert set.version == "0.0.3"
    if current_env is not None:
        os.environ["INBOUND_VERSION"] = current_env

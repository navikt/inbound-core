import logging

import pytest
from _pytest.logging import caplog as _caplog
from loguru import logger

from inbound.core.logging import LOGGER


@pytest.fixture
def caplog(caplog):
    handler_id = logger.add(caplog.handler, format="{message}")
    yield caplog
    logger.remove(handler_id)


def log_oda():
    LOGGER.info("Oda")


def test_log(caplog):
    with caplog.at_level(logging.INFO):
        log_oda()
    print(caplog.text)
    print(caplog)
    assert "Oda" in caplog.text

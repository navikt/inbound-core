from urllib.parse import urljoin

import pytest
import requests
from urllib3 import Retry


@pytest.fixture(scope="function")
def wait_for_container():
    """Wait for the api to become responsive"""
    session = requests.Session()
    request_session = requests.Session()
    retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
    request_session.mount("http://", requests.adapters.HTTPAdapter(max_retries=retries))
    api_url = "http://localhost:3003"
    assert request_session.get(api_url)
    return request_session, api_url


def test_read(wait_for_container):
    request_session, api_url = wait_for_container
    item = request_session.get(urljoin(api_url, "hello"))
    assert item.text == '"Hello"'

import os
from pathlib import Path

from inbound.artifacts.dbt import manifest


def test_parse_manifest(data_path):

    path = data_path + "/dbt/target/manifest.json"
    res = manifest.parse_manifest(path)

    assert isinstance(res.nodes, dict)

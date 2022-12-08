import json
from pathlib import Path

from inbound.dbt_artifacts.graph import write_lineage_graph


def test_write_lineage_graph(data_path):

    manifest_file = data_path + "/dbt/target/manifest.json"
    out_file = data_path + "/lineage/graph.json"
    write_lineage_graph(manifest_file, out_file)

    with open(out_file, "r") as f:
        graph_json = json.load(f)
        datasets = graph_json["datasets"]
        assert len(datasets) == 8

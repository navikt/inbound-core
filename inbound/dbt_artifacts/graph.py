import json
import os
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Protocol, Tuple

from inbound.dbt_artifacts import manifest


def filter_nodes(values):
    return {
        k: v
        for k, v in values.items()
        if v.resource_type.value in ("model", "seed", "source", "exposure")
    }


class Node(Protocol):
    depends_on: List[str]
    sources: List[List[str]]
    database: str
    schema_: str
    name: str
    items: List[Any]
    resource_type: str


class Model(Protocol):
    nodes: Dict[str, Node]
    sources: Dict[str, Node]
    metadata: Dict


class Graph:
    def __init__(self, model: Model, meta: dict = None):
        self.model = model
        self.meta = meta
        self.nodes = filter_nodes(self.model.nodes)
        self.sources = filter_nodes(self.model.sources)

    def url(self, node: Node) -> str:
        return f"{node.database}.{node.schema_}.{node.name}"

    @property
    def all(self) -> List[Node]:
        return {**self.nodes, **self.sources}

    def node_list(self) -> List[Node]:
        nodes = [(self.url(v), self.get_node_dict(v)) for k, v in self.nodes.items()]
        sources = [
            (self.url(v), self.get_node_dict(v)) for k, v in self.sources.items()
        ]
        return nodes + sources

    def node_url_from_id(self, id: str) -> str:
        return self.model.nodes[id].url

    def edge_list(self) -> List[Tuple[str, str]]:
        return [
            (self.url(v), self.url(self.all[d]))
            for k, v in self.model.nodes.items()
            for d in v.depends_on.nodes
        ]

    def get_node_dict(self, node: Node) -> Dict:
        url = self.url(node)
        meta = json.loads(node.json(include={"name", "description", "meta"}))
        meta["url"] = url
        return meta

    def build_graph(self) -> Dict:
        g = {}
        g["datasets"] = self.node_list()
        g["edges"] = self.edge_list()
        g["meta"] = self.meta
        return g


def get_lineage_graph(path: str) -> Dict:

    _path = (
        path
        or (os.environ["INBOUND_DBT_PROJECT_PATH"] + "/target/manifest.json")
        or (Path.cwd() / "dbt" / "target/manifest.json")
    )

    model = manifest.parse_manifest(_path)

    m = Graph(model)
    g = m.build_graph()

    return json.dumps(g)


def write_lineage_graph(manifest_file: str, out_file: str) -> None:

    content = get_lineage_graph(manifest_file)

    with open(out_file, "w+") as f:
        f.write(content)

import json

from inbound.dbt_artifacts.models.manifest_model_v5 import Model as Model_v5
from inbound.dbt_artifacts.models.manifest_model_v6 import Model as Model_v6
from inbound.dbt_artifacts.models.manifest_model_v7 import Model as Model_v7


def parse_manifest(path):
    with open(path, "r") as fp:
        artifact = json.load(fp)

        if artifact.get("metadata") is not None:
            if artifact.get("metadata").get("dbt_schema_version") is not None:
                version = (
                    artifact.get("metadata")
                    .get("dbt_schema_version")
                    .split("/")[-1][:2]
                )
                parser = {"v5": Model_v5, "v6": Model_v6, "v7": Model_v7}
                res = parser[version](**artifact)
                return res

    return None

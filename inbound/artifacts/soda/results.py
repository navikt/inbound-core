import json

from inbound.artifacts.soda.models.results_model import Model


def parse_results(path):
    try:
        with open(path, "r") as fp:
            artifact = json.load(fp)
            res = Model(**artifact)
            return res
    except:
        return None

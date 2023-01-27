import json

import yaml

import inbound.core.soda_profile as soda_profile
from inbound.core.models import SodaProfile, SodaSpec


def test_get_soda_profile():
    profile = soda_profile.soda_profile(
        "data_source eyeshare", "snowflake_test", "loader"
    )
    profile_json = json.loads(profile.json())
    assert profile_json["data_source eyeshare"]["type"] == "snowflake"


def test_get_soda_yaml():
    profile = soda_profile.soda_profile_yml(
        "data_source eyeshare", "snowflake_test", "loader"
    )
    profile_json = yaml.safe_load(profile)
    assert profile_json["data_source eyeshare"]["type"] == "snowflake"


test_get_soda_profile()
test_get_soda_yaml()

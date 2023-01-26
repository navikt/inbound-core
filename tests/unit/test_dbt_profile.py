import os

from snowflake.sqlalchemy import URL

from inbound.core.dbt_profile import DbtProfile, dbt_config, dbt_connection_params
from inbound.core.logging import LOGGER
from inbound.core.models import Spec

sf_conn_params = [
    "account",
    "region",
    "user",
    "password",
    "database",
    "warehouse",
    "role",
    "schema",
]


def test_dbt_profile():
    dbt_profile = DbtProfile()
    profile = dbt_profile.profile.elements["jaffle_shop"]
    assert profile["target"] == "dev"


def test_dbt_get_profile_target():
    dbt_profile = DbtProfile()
    profile_target = dbt_profile.profile.get_profile_target("jaffle_shop")
    assert profile_target == "dev"


def test_dbt_connection_spec():
    dbt_profile = DbtProfile()
    spec = dbt_profile.profile.get_connection_spec("jaffle_shop")
    assert spec["type"] == "duckdb"


def test_dbt_get_profile():
    dbt_profile = DbtProfile()
    item = dbt_profile.profile.get_profile("jaffle_shop")
    assert item["target"] == "dev"


def test_get_dbt_connection_params():
    outputs = dbt_connection_params("jaffle_shop", "dev")
    assert outputs != {}
    type = outputs.get("type")
    assert type == "duckdb"


def test_get_dbt_config():
    config = dbt_config()
    assert config != {}
    stats = config.get("send_anonymous_usage_stats")
    assert stats == False


def test_sf_connection():

    LOGGER.info(f"os.environ[DBT_PROFILES_DIR] = {os.getenv('DBT_PROFILES_DIR')}")

    spec = Spec(profile="snowflake_test", target="loader")

    if spec.connection_string is not None:
        assert True
        return

    if spec.profile and spec.target:
        try:
            params = dbt_connection_params(spec.profile, spec.target, spec.profiles_dir)
            spec.connection_string = URL(**params)
            return
        except Exception as e:
            LOGGER.error(
                f"Error reading dbt profile for profile={spec.profile} and target={spec.target} with profiles_dir {spec.profiles_dir}. Error: {e}"
            )
            assert False

    params = dict()
    spec_dict = spec.dict(by_alias=True)
    for param in sf_conn_params:
        if param in spec_dict.keys():
            params[param] = spec_dict.get(param)

    try:
        spec.connection_string = URL(**params)
    except Exception as e:
        LOGGER.error("Error creating connection string from {spec}. {e}")
        assert False

    assert True


test_sf_connection()

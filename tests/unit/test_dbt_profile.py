from inbound.core.dbt_profile import (
    DbtProfile,
    get_dbt_config,
    get_dbt_connection_params,
)


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
    outputs = get_dbt_connection_params("jaffle_shop", "dev")
    assert outputs != {}
    type = outputs.get("type")
    assert type == "duckdb"


def test_get_dbt_config():
    config = get_dbt_config()
    assert config != {}
    stats = config.get("send_anonymous_usage_stats")
    assert stats == False


# test_dbt_profile()

"""
jaffle_shop:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: '../.data/jaffle_shop.duckdb'
      threads: 24

snowflake_eyeshare:
  target: loader
  outputs:
    loader:
      type: "snowflake"
      account: "{{env_var('SNOWFLAKE_DATAPRODUCT_ACCOUNT')}}.{{env_var('SNOWFLAKE_DATAPRODUCT_REGION')}}"
      user: "{{env_var('SNOWFLAKE_DATAPRODUCT_USER')}}" 
      password: "{{env_var('SNOWFLAKE_DATAPRODUCT_PASSWORD')}}"
      role: "vdl_eyeshare_loader"
      database: "vdl_eyeshare"
      warehouse: "vdl_eyeshare_loading"
      schema:  "raw"
      threads: 1
      client_session_keep_alive: False
      query_tag: "eyeshare"
"""

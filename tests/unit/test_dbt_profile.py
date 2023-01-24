from inbound.core.dbt_profile import DbtProfile, dbt_config, dbt_connection_params


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

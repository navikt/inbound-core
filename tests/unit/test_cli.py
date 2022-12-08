from click.testing import CliRunner

from inbound.framework.cli import inbound


def test_inbound_info():
    runner = CliRunner()
    result = runner.invoke(inbound, ["info"])
    assert result.exit_code == 0


def test_inbound_run_all():
    runner = CliRunner()
    result = runner.invoke(inbound, ["run"])
    assert result.exit_code == 0


def test_inbound_run_job(data_path):
    runner = CliRunner()
    result = runner.invoke(
        inbound, ["run", "--project-dir", data_path, "--job", "csv_duckdb.yml"]
    )
    assert result.exit_code == 0

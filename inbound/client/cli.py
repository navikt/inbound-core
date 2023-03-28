import os
import webbrowser
from pathlib import Path

import click
import pandas as pd
from pygit2 import Repository

import inbound.core.dbt_profile as dbt_profile
from inbound import __version__ as version
from inbound.core.jobs import run_job, run_jobs
from inbound.core.models import Profile, Spec
from inbound.plugins.connections import SnowflakeConnection

here = os.getcwd()

LOGO = rf"""
inbound. v{version}
"""


@click.group(name="inbound")
@click.version_option(version, "--version", "-V", help="Show version and exit")
def inbound():
    """inbound is a CLI for running data ingestion and governance jobs. For more
    information, type ``inbound info``.
    """
    pass


@inbound.command()
def info():
    """Get more information about inbound."""
    click.secho(LOGO, fg="green")
    click.echo("Declarative data ingestion for dataproducts.")


@inbound.command(short_help="See the API docs and introductory tutorial.")
def docs():
    """Display the API docs and introductory tutorial in the browser,
    using the packaged HTML doc files."""
    html_path = str((Path(__file__).parent.parent / "html" / "index.html").resolve())
    index_path = f"file://{html_path}"
    click.echo(f"Opening {index_path}")
    webbrowser.open(index_path)


@inbound.command()
@click.option(
    "--profiles-dir", envvar="INBOUND_PROFILES_DIR", default="./inbound", required=False
)
@click.option(
    "--project-dir",
    envvar="inbound_PROJECT_DIR",
    default="./inbound/jobs",
    required=False,
)
@click.option("--job", default=None, help="Job to be run", required=False)
def run(profiles_dir, project_dir, job):
    dir = here
    if Path(project_dir).is_dir():
        dir = project_dir
    else:
        dir = os.path.join(here, project_dir)

    if job:  # run single job
        source = os.path.join(dir, job)
        click.echo(f"run job: {source}")
        return run_job(source, profiles_dir)
    else:  # run all jobs in jobs directory
        click.echo(f"run all jobs in directory: {dir}")
        return run_jobs(dir, profiles_dir)


@inbound.command
@click.option("--profile", default=None, required=True)
@click.option("--target", default="constructor", required=False)
@click.option(
    "--profiles_dir",
    default="./src/dbt",
    required=False,
)
def clone(**user_input) -> None:
    dbt_profile_params = dbt_profile.dbt_connection_params(**user_input)
    spec = Spec(**dbt_profile_params)
    profile = Profile(type="snowflake", name=f"snowflake", spec=spec)

    prefix = Repository(".").head.shorthand.replace("-", "_")
    original_db = spec.database
    cloned_db = f"{prefix}_{original_db}"

    query = f"show grants on database regnskap"
    with SnowflakeConnection(profile=profile) as db:
        db.execute(f"create or replace database {cloned_db} clone {original_db}")
        df = pd.read_sql(sql=query, con=db.engine)
        df = df[df["privilege"] == "USAGE"]
        for grantee in df["grantee_name"].__iter__():
            db.execute(f"grant usage on database {cloned_db} to role {grantee}")


def main():
    inbound()


if __name__ == "__main__":
    main()

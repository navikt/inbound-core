import json
import os
import tempfile
import time
from pathlib import Path
from typing import Union

import yaml
from jinja2 import Template

from inbound.core import connection_factory, connection_loader
from inbound.core.environment import get_env
from inbound.core.job_factory import JobFactory
from inbound.core.job_result import JobResult
from inbound.core.logging import LOGGER
from inbound.core.models import *
from inbound.core.utils import generate_id


def run_all_job_in_directory(path: str = "./jobs", profiles_dir: Path = None):

    """Set run metadata"""
    os.environ["INBOUND_RUN_ID"] = str(round(time.time() * 1000))

    with tempfile.TemporaryDirectory() as tmpdirname:
        os.environ["INBOUND_SECRET_DIR"] = tmpdirname
        try:
            jobs = [
                os.path.join(d, x)
                for d, dirs, files in os.walk(path)
                for x in files
                if x.endswith(".yml")
            ]
        except Exception as e:
            LOGGER.info(
                f"Error in searching for job .yml files i path: {path}. {str(e)}"
            )
            return JobResult()

        ret = JobResult()
        for job in jobs:
            res = run_job(job)
            if res.result != "DONE":
                LOGGER.info(f"Error running job: {job}. Result: {str(ret)}")
            ret.result = res.result
            ret.append(res.to_json())

        return ret


def get_json_config(source: Union[str, dict]):

    if type(source) == dict:
        return source

    try:
        jobs_spec = json.loads(source)
        LOGGER.info(f"Loaded jobs configuration from json {source}")
        return jobs_spec
    except:
        pass

    if not Path(source).is_file():
        source = str(Path.cwd() / source)

    try:
        with open(source, "r") as f:
            # default format is yaml
            try:
                jobs_spec = yaml.safe_load(f)
                return jobs_spec
            except:
                # try alternative format: json
                try:
                    jobs_spec = json.load(f)
                    return jobs_spec
                except:
                    pass

        LOGGER.info(f"Loaded jobs configuration from {source}")
    except Exception as e:
        LOGGER.info(f"Error loading jobs configuration from {source}. {e}")


def run_job(source: Union[str, dict], profiles_dir: Path = None) -> JobResult:

    try:
        jobs_spec = get_json_config(source)
    except Exception as e:
        LOGGER.error(f"Error reading job config from {source}. {e}")

    # Replace 'env_var's in template
    try:
        temp = Template(json.dumps(jobs_spec)).render(env_var=get_env)
    except Exception as e:
        LOGGER.error(f"Error setting env variables. {e}")

    try:
        jobs_config = json.loads(temp, strict=False)
    except Exception as e:
        LOGGER.error(f"Error parsing job config. {e}")

    # Parse json
    try:
        jobs = JobsModel(**jobs_config).jobs
    except Exception as e:
        LOGGER.error(f"read_datas configuration. {e}")
        return JobResult()

    # Load plugins for source og target
    source_types = [job.source.type for job in jobs]
    sink_types = [job.target.type for job in jobs]
    types = list(set(source_types + sink_types))
    connection_loader.load_plugins(types)

    # Run E(T)L jobs
    ret = JobResult()
    for job in jobs:
        job.job_id = generate_id()
        start_time = time.monotonic()
        LOGGER.info(
            f"Starting job: {job.name} ({job.job_id}). Source: {job.source.name or job.source.type}. Target: {job.target.name or job.target.type}"
        )
        source_connector = connection_factory.create(job.source)
        sink_connector = connection_factory.create(job.target)
        job_instance = JobFactory(source_connector, sink_connector, job)()
        try:
            res = job_instance.run()
            res.duration_seconds = time.monotonic() - start_time
            ret.result = res.result
            ret.append(res)
            LOGGER.info(
                f"Job {job.name} ({job.job_id}) completed in {str(res.duration_rounded)} seconds. Result: {str(res)}"
            )
        except Exception as e:
            duration = (time.monotonic() - start_time) // 1000000
            LOGGER.info(
                f"Job {job.name} ({job.job_id}) failed after {str(duration)} seconds. Exception {str(e)}"
            )
            pass

    return ret

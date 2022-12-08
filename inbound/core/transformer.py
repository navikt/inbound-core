import importlib
import time
from typing import Tuple

import pandas

from inbound.core.job_result import JobResult
from inbound.core.logging import LOGGER
from inbound.core.models import Spec


def transform(
    spec: Spec, df: pandas.DataFrame, job_id: str = None
) -> Tuple[pandas.DataFrame, JobResult]:

    start_time = time.monotonic_ns()
    job_result = JobResult(job_id=job_id)

    if spec.transformer is None:
        job_result.duration_ns = time.monotonic_ns() - start_time
        job_result.result = "DONE"
        return df, job_result
    else:
        try:
            transformer = _get_transformer(spec.transformer)
            job_result.duration_ns = time.monotonic_ns() - start_time
            job_result.result = "DONE"
            return transformer.transform(df), job_result
        except Exception as e:
            job_result.duration_ns = time.monotonic_ns() - start_time
            job_result.result = "FAILED"
            LOGGER.info(f"Error transforming dataframe. {e}")
            return df, job_result


def _get_transformer(path: str):

    module_name = "transformer"
    spec = importlib.util.spec_from_file_location(module_name, path)

    if spec is None:
        LOGGER.info(f"Could not find module {module_name} in path {path}")
    else:
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        return module

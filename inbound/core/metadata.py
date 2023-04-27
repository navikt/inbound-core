import datetime
import hashlib
import json
import time
import tracemalloc
from collections import defaultdict
from typing import Tuple

import pandas

from inbound.core.job_result import JobResult
from inbound.core.logging import LOGGER
from inbound.core.models import Spec
from inbound.core.package import get_pacage_name, get_package_version


def enriched_with_metadata(
    spec: Spec, df: pandas.DataFrame, job_id: str = None
) -> Tuple[pandas.DataFrame, JobResult]:

    start_date_time = datetime.datetime.now()
    job_res = JobResult(job_id=job_id, start_date_time=start_date_time)
    if df.empty:
        return df, job_res

    if spec.format == "meta+json" and type(spec.meta) == defaultdict:
        df_out = pandas.DataFrame(index=range(len(df)))
        for key, value in spec.meta.items():
            df_out[key] = str(value)
        df_out["loaded"] = start_date_time
        df_out["data"] = df.to_dict("records")

        job_res.task_name = "Process: meta+json"
        job_res.end_date_time = datetime.datetime.now()
        job_res.memory = tracemalloc.get_traced_memory()
        job_res.result = "DONE"

        return df_out, job_res

    if spec.format == "meta" and type(spec.meta) == defaultdict:
        df_out = pandas.DataFrame()
        for key, value in spec.meta.items():
            df_out[key] = value
        df_out["loaded"] = start_date_time

        job_res.task_name = "Process: meta"
        job_res.end_date_time = datetime.datetime.now()
        job_res.memory = tracemalloc.get_traced_memory()
        job_res.result = "DONE"

        return df_out.concat(df), job_res

    if spec.format == "log":

        try:
            df_out = pandas.DataFrame()

            if spec.row_id:
                if type(spec.row_id) is str:
                    df_out["ROW_ID"] = df[spec.row_id]
                elif all(isinstance(s, str) for s in spec.row_id):
                    df_out["ROW_ID"] = (
                        df[[x for x in df.columns if x in spec.row_id]]
                        .apply(lambda x: "_".join(x.astype(str)), axis=1)
                        .replace(" ", "_")
                    )
            else:
                for id_col in ["id", "ID"]:
                    if id_col in df.columns:
                        df_out["ROW_ID"] = df[id_col]
            df_out["RAW"] = df.to_json(
                orient="records", lines=True, force_ascii=False, date_format="iso"
            ).splitlines()

            df_out["SOURCE"] = spec.source
            df_out["INTERFACE"] = spec.interface
            df_out["LOADER"] = get_pacage_name() + "-" + get_package_version()
            df_out["JOB_ID"] = job_id
            df_out["LOAD_TIME"] = datetime.datetime.now().timestamp()
            df_out["HASH"] = [
                hashlib.md5(data.encode("utf-8")).hexdigest() for data in df_out["RAW"]
            ]

            job_res.task_name = "Process: log"
            job_res.end_date_time = datetime.datetime.now()
            job_res.memory = tracemalloc.get_traced_memory()
            job_res.result = "DONE"

            df = df_out
            return df, job_res

        except Exception as e:
            LOGGER.error(f"Error converting dataframe to log format. {e}")
            return pandas.DataFrame, JobResult("FAILED")

    else:
        job_res.task_name = "Process: skip"
        job_res.end_date_time = datetime.datetime.now()
        job_res.memory = tracemalloc.get_traced_memory()
        job_res.result = "DONE"
        return df, job_res

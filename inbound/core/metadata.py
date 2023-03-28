import datetime
import hashlib
import json
import time
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
    start_time = time.monotonic()
    job_res = JobResult(job_id=job_id, start_date_time=start_date_time)

    if spec.format == "meta+json" and type(spec.meta) == defaultdict:
        df_out = pandas.DataFrame(index=range(len(df)))
        for key, value in spec.meta.items():
            df_out[key] = str(value)
        df_out["loaded"] = start_date_time
        df_out["data"] = df.to_dict("records")

        job_res.duration_seconds = time.monotonic() - start_time
        job_res.result = "DONE"

        return df_out, job_res

    if spec.format == "meta" and type(spec.meta) == defaultdict:
        df_out = pandas.DataFrame()
        for key, value in spec.meta.items():
            df_out[key] = value
        df_out["loaded"] = start_date_time

        job_res.duration_seconds = time.monotonic() - start_time
        job_res.result = "DONE"

        return df_out.concat(df), job_res

    if spec.format == "log":

        try:
            df_out = pandas.DataFrame()

            if spec.row_id:
                if type(spec.row_id) is str:
                    df_out["row_id".upper()] = df[spec.row_id]
                elif all(isinstance(s, str) for s in spec.row_id):
                    df_out["row_id".upper()] = (
                        df[[x for x in df.columns if x in spec.row_id]]
                        .apply(lambda x: "_".join(x.astype(str)), axis=1)
                        .replace(" ", "_")
                    )
            df_out["raw".upper()] = df.to_json(
                orient="records", lines=True, force_ascii=False, date_format="iso"
            ).splitlines()

            df_out["source".upper()] = spec.source
            df_out["interface".upper()] = spec.interface
            df_out["loader".upper()] = get_pacage_name() + "-" + get_package_version()
            df_out["job_id".upper()] = job_id
            df_out["load_time".upper()] = datetime.datetime.now().timestamp()
            df_out["hash".upper()] = [
                hashlib.md5(data.encode("utf-8")).hexdigest() for data in df_out["raw".upper()]
            ]

            job_res.duration_seconds = time.monotonic() - start_time
            job_res.result = "DONE"

            return df_out, job_res

        except Exception as e:
            LOGGER.error(f"Error converting dataframe to log format. {e}")
            return pandas.DataFrame, JobResult("FAILED")

    else:
        job_res.duration_seconds = time.monotonic() - start_time
        job_res.result = "DONE"
        return df, job_res

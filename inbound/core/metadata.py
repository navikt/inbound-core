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

# lastet_session: "hvordan kom jeg over - for eksempel batch last med <navn på app>"
# kildesystem: "<OeBS, Lydia, etc.>"
# grensesnitt: "hva slags grensesnitt kommer dataene fra, for eksempel, views i kilde database etc"
# lastet_tid: tidspunkt når dataene kommer i til oss


def enriched_with_metadata(
    spec: Spec, df: pandas.DataFrame, job_id: str = None
) -> Tuple[pandas.DataFrame, JobResult]:

    start_date_time = datetime.datetime.now()
    start_time = time.monotonic_ns()
    job_res = JobResult(job_id=job_id, start_date_time=start_date_time)

    if spec.format == "meta+json" and type(spec.meta) == defaultdict:
        df_out = pandas.DataFrame(index=range(len(df)))
        for key, value in spec.meta.items():
            df_out[key] = str(value)
        df_out["loaded"] = start_date_time
        df_out["data"] = df.to_dict("records")

        job_res.duration_ns = time.monotonic_ns() - start_time
        job_res.result = "DONE"

        return df_out, job_res

    if spec.format == "meta" and type(spec.meta) == defaultdict:
        df_out = pandas.DataFrame()
        for key, value in spec.meta.items():
            df_out[key] = value
        df_out["loaded"] = start_date_time

        job_res.duration_ns = time.monotonic_ns() - start_time
        job_res.result = "DONE"

        return df_out.concat(df), job_res

    if spec.format == "log":

        try:
            df_out = pandas.DataFrame()

            if spec.row_id:
                if type(spec.row_id) is str:
                    df_out["row_id"] = df[spec.row_id]
                elif all(isinstance(s, str) for s in spec.row_id):
                    df_out["row_id"] = (
                        df[[x for x in df.columns if x in spec.row_id]]
                        .apply(lambda x: "_".join(x.astype(str)), axis=1)
                        .replace(" ", "_")
                    )

            df_out["raw"] = df.astype(str).to_dict(orient="records")
            df_out["raw"] = df_out["raw"].apply(lambda x: json.dumps(x))
            df_out["source"] = spec.source
            df_out["interface"] = spec.interface
            df_out["loader"] = get_pacage_name() + "-" + get_package_version()
            df_out["job_id"] = job_id
            df_out["timestamp"] = datetime.datetime.now().timestamp()
            df_out["hash"] = [
                hashlib.md5(data.encode("utf-8")).hexdigest() for data in df_out["raw"]
            ]

            job_res.duration_ns = time.monotonic_ns() - start_time
            job_res.result = "DONE"

            return df_out, job_res

        except Exception as e:
            LOGGER.error(f"Error converting dataframe to log format. {e}")
            return pandas.DataFrame, JobResult("FAILED")

    else:
        job_res.duration_ns = time.monotonic_ns() - start_time
        job_res.result = "DONE"
        return df, job_res

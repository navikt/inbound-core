import datetime
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import ForwardRef, List, Optional, Tuple

from pydantic import BaseModel, validator

from inbound.core.job_result import JobResult
from inbound.core.logging import LOGGER
from inbound.core.utils import generate_id, get_target_dir


class JobsResult(BaseModel):
    """Observability parameters from running jobs."""

    result: Optional[str] = "NO_RUN"
    job_id: Optional[str] = ""
    job_name: Optional[str] = ""
    jobs: Optional[List[JobResult]] = []
    start_date_time: Optional[datetime.datetime] = datetime.datetime.now()
    end_date_time: Optional[datetime.datetime] = datetime.datetime.now()
    memory: Optional[Tuple[float, float]] = 0, 0

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.job_id is None:
            self.job_id = generate_id()

    @property
    def success(self):
        return self.result == "DONE"

    @property
    def duration_seconds(self):
        return (self.end_date_time - self.start_date_time).total_seconds()

    @property
    def duration_rounded(self):
        return round(self.duration_seconds, 4)

    @property
    def memory_size(self):
        return self.memory[0]

    @property
    def memory_peak(self):
        return self.memory[1]

    def append(self, res: JobResult):
        self.jobs.append(res)
        self.memory = (
            max(self.memory_size, res.memory_size),
            max(self.memory_size, res.memory_size),
        )

    def to_json(self):
        jobs = []
        for job in self.jobs:
            jobs.append(job.to_json())
        return {
            "id": self.job_id,
            "name": self.job_name,
            "result": self.result,
            "start": self.start_date_time,
            "duration": str(self.duration_rounded),
            "memory size": str(self.memory_size),
            "memory peak": str(self.memory_peak),
            "jobcount": str(len(self.jobs)),
            "jobs": jobs,
        }

    def __str__(self):
        res = f"""Jobs: {self.result}. Id: {self.job_id}. Name: {self.job_name}. Start: {self.start_date_time.strftime('%Y-%m-%d %H:%M:%S')}. Duration: {str(self.duration_rounded)}. Memory: {str(self.memory_size)}/{str(self.memory_peak)}. Job count: {len(self.jobs)}"""

        return res

    def log(self):
        LOGGER.info(str(self))

        json_str = json.dumps(self.to_json(), default=str)
        with open(str(Path(get_target_dir() / "job_results.json")), "a+") as log_file:
            log_file.write(json_str)


JobsResult.update_forward_refs()

import datetime
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import ForwardRef, List, Optional, Tuple

from pydantic import BaseModel, validator

from inbound.core.logging import LOGGER
from inbound.core.utils import generate_id, get_target_dir

LOCAL_TIMEZONE = datetime.datetime.now().astimezone().tzinfo

JobResult = ForwardRef("JobResult")


class JobResult(BaseModel):
    """Observability parameters from a job run."""

    result: Optional[str] = "NO_RUN"
    job_id: Optional[str] = ""
    job_name: Optional[str] = ""
    task_name: Optional[str] = ""
    rows: Optional[int] = -1
    size: Optional[int] = -1
    chunk_number: Optional[int] = -1
    start_date_time: Optional[datetime.datetime] = datetime.datetime.now()
    end_date_time: Optional[datetime.datetime] = datetime.datetime.now()
    batches: Optional[List[JobResult]] = []
    memory: Optional[Tuple[float, float]] = 0, 0
    exception: Optional[str] = ""

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

    @property
    def timezone(self):
        return LOCAL_TIMEZONE

    def append(self, res: JobResult):
        self.result = res.result
        self.rows += res.rows
        self.size += res.size
        self.duration_seconds += res.duration_seconds
        self.memory = tuple(
            [
                max(0, self.memory[0], res.memory[0]),
                max(0, self.memory[1], res.memory[1]),
            ]
        )
        self.batches.append(res)

    def to_json(self):
        batches = []
        for batch in self.batches:
            batches.append(batch.to_json())
        return {
            "id": self.job_id,
            "name": self.job_name,
            "task": self.task_name,
            "result": self.result,
            "rows": str(self.rows),
            "size": str(self.size),
            "start": self.start_date_time,
            "duration": str(self.duration_rounded),
            "memory size": str(self.memory_size),
            "memory peak": str(self.memory_peak),
            "batchcount": str(len(self.batches)),
            "batches": batches,
        }

    def __str__(self):
        res = f"""Job: {self.result}. {self.job_id}: {self.task_name}. Rows: {self.rows}. Chunk: {self.chunk_number}. Start: {self.start_date_time.strftime('%Y-%m-%d %H:%M:%S')}. Duration: {str(self.duration_rounded)}. Memory: {str(self.memory_size)}/{str(self.memory_peak)}"""
        return res

    def log(self):
        LOGGER.info(str(self))

        json_str = json.dumps(self.to_json(), default=str)
        with open(str(Path(get_target_dir() / "job_result.json")), "a+") as log_file:
            log_file.write(json_str)


JobResult.update_forward_refs()

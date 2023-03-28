import datetime
import json
from dataclasses import dataclass, field
from typing import ForwardRef, List, Optional

from pydantic import BaseModel, validator

LOCAL_TIMEZONE = datetime.datetime.now().astimezone().tzinfo

JobResult = ForwardRef("JobResult")

# TODO: use second instead of ns resolution to avoid risk of overflow?
class JobResult(BaseModel):
    """Observability parameters from a job run."""

    result: Optional[str] = "NO_RUN"
    job_id: Optional[str] = ""
    rows: Optional[int] = 0
    size: Optional[int] = 0
    start_date_time: Optional[datetime.datetime] = datetime.datetime.now()
    end_date_time: Optional[datetime.datetime] = datetime.datetime.now()
    duration_seconds: Optional[int] = 0
    batches: Optional[List[JobResult]] = []
    memory: Optional[int] = 0

    @property
    def success(self):
        return self.result == "DONE"

    @property
    def duration_seconds(self):
        if self.duration_seconds:
            return self.duration_seconds
        else:
            duration = self.end_date_time - self.start_date_time
            return duration.total_seconds()

    @property
    def timezone(self):
        return LOCAL_TIMEZONE

    def to_json(self):
        return {
            "result": self.result,
            "rows": str(self.rows),
            "size": str(self.size),
            "duration": str(self.duration_seconds),
            "batchcount": str(len(self.batches)),
        }

    def append(self, res: JobResult):
        self.result = res.result
        self.rows += res.rows
        self.size += res.size
        self.duration_seconds += res.duration_seconds
        self.batches.append(res)
        self.memory = max(self.memory, res.memory)

    def __str__(self):
        return f"Finished in {self.duration_seconds} seconds. Result: {json.dumps(self.to_json())}"


JobResult.update_forward_refs()

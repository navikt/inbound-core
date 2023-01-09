import datetime
import time
from dataclasses import dataclass

from inbound.core.job_id import generate_id
from inbound.core.job_result import JobResult
from inbound.core.logging import LOGGER
from inbound.core.metadata import enriched_with_metadata
from inbound.core.models import JobModel
from inbound.core.transformer import transform
from inbound.plugins.connections.connection import Connection


@dataclass
class Job:
    source: Connection
    sink: Connection
    config: JobModel

    def run(self) -> JobResult:
        job_id = self.config.job_id
        start_time_job_ns = time.monotonic_ns()
        job_result = JobResult(job_id=job_id, start_date_time=datetime.datetime.now())

        try:
            with self.source as source:
                with self.sink as sink:
                    iterator = source.to_pandas(job_id)
                    for index, tuple_res in enumerate(iterator):
                        df = tuple_res[0]
                        start_time_batch = time.monotonic_ns()

                        # transform dataframe if specified
                        df, transform_job_result = transform(
                            source.profile.spec, df, job_id
                        )
                        job_result.append(transform_job_result)

                        # add metadata if specified
                        df, metadata_job_result = enriched_with_metadata(
                            source.profile.spec, df, job_id
                        )
                        job_result.append(metadata_job_result)

                        # write to sink
                        result, batch_job_result = sink.from_pandas(
                            df,
                            chunk=index,
                            mode=self.sink.profile.spec.mode,
                            job_id=job_id,
                        )
                        batch_job_result.duration_ns = (
                            time.monotonic_ns() - start_time_batch
                        )
                        job_result.append(batch_job_result)

            job_result.result = "DONE"
        except Exception as e:
            LOGGER.error(f"Error running job. {e}")
            job_result.result = "FAILED"

        finally:
            job_result.duration_ns = time.monotonic_ns() - start_time_job_ns
            job_result.end_date_time = datetime.datetime.now()
            return job_result


@dataclass
class JobFactory:
    source_class: Connection
    sink_class: Connection
    config: JobModel

    def __call__(self) -> Job:
        source = self.source_class
        sink = self.sink_class
        config = self.config
        # validate_job_schema(config)
        return Job(source, sink, config)


def run(job: Job) -> JobResult:
    return job.run()

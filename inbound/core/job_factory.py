import datetime
import os
import tracemalloc
from dataclasses import dataclass

import inbound.core.profiler as profiler
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
        job_name = self.config.name
        job_result = JobResult(
            job_id=job_id,
            job_name=job_name,
            start_date_time=datetime.datetime.now(),
            task_name="Job",
        )

        tracemalloc.start()

        try:
            with self.source as source:
                with self.sink as sink:
                    iterator = source.to_pandas(job_id)
                    for index, (df, read_res) in enumerate(iterator):
                        # log result of data loading
                        read_res.log()

                        # transform dataframe if specified
                        if source.profile.spec.transformer is not None:
                            df, transform_job_result = transform(
                                source.profile.spec, df, job_id
                            )
                            # log result of data transformation
                            transform_job_result.log()

                        # add metadata if specified
                        if source.profile.spec.format is not None:
                            df, metadata_job_result = enriched_with_metadata(
                                source.profile.spec, df, job_id
                            )
                            # log result of data enrichments
                            metadata_job_result.log()

                        # write to sink
                        _, batch_job_result = sink.from_pandas(
                            df,
                            chunk_number=index,
                            mode=sink.profile.spec.mode,
                            job_id=job_id,
                        )

                        if os.getenv("INBOUND_PROFILING") is not None:
                            profiler.snapshot()
                        # log result persisting data
                        batch_job_result.log()

            job_result.result = "DONE"
        except Exception as e:
            LOGGER.error(f"Error running job. {e}")
            job_result.result = "FAILED"

        finally:
            if os.getenv("INBOUND_PROFILING") is not None:
                profiler.display_stats()
                profiler.compare()
                profiler.print_trace()
            job_result.end_date_time = datetime.datetime.now()
            job_result.memory = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            job_result.log()
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

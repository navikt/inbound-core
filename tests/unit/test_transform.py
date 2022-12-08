import pandas

from inbound.core.job_result import JobResult
from inbound.core.logging import LOGGER
from inbound.core.models import Spec
from inbound.core.transformer import transform
from inbound.plugins.utils import df


def test_transformer(data_path):

    spec = Spec(transformer=data_path + "/transformer.py")
    df_out, job_result = transform(spec, df)

    assert type(df_out) == pandas.DataFrame
    assert type(job_result) == JobResult


if __name__ == "__main__":
    test_transformer("/Users/paulbencze/Projects/inbound/data")

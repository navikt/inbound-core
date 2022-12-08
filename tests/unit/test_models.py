import yaml

from inbound.core.models import JobsModel, Profile, Spec


def test_creds_model():

    spec = Spec(
        name="db",
        connection_string="sqlite://",
        table="test_table",
        query="select * from test_table",
    )
    profile = Profile(type="sqllite", spec=spec)

    assert profile.name == spec.name


def test_job_model_yml(data_path):
    with open(data_path + "/jobs.yml") as src:
        yml_doc = src.read()
        js_doc = yaml.load(yml_doc, yaml.BaseLoader)

        inbound = JobsModel(**js_doc)

        assert len(inbound.jobs) > 0

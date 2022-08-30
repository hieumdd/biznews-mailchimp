import pytest

from mailchimp.pipeline import pipelines
from mailchimp import coffeehr_service


class TestCoffeeHR:
    @pytest.fixture(
        params=pipelines.values(),
        ids=pipelines.keys(),
    )
    def pipeline(self, request):
        return request.param

    def test_pipeline_service(self, pipeline):
        res = coffeehr_service.pipeline_service(pipeline)
        assert res >= 0

    @pytest.mark.parametrize("sector", ["VP", "CH", "VC"])
    def test_employee_service(self, sector):
        res = coffeehr_service.employee_service(sector)
        assert res


class TestTasks:
    def test_service(self):
        res = coffeehr_service.create_tasks_service()
        assert res["tasks"] > 0

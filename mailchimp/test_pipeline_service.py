import pytest

from mailchimp import pipeline_service


@pytest.mark.parametrize(
    "service",
    [pipeline_service.get_lists_service, pipeline_service.get_campaigns_service],
    ids=["get_lists", "get_campaigns"],
)
def test_pipeline_service(service):
    res = service()
    assert res

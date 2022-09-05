import pytest

from mailchimp.pipeline import primary_pipeline_service, export_pipeline_service


class TestPrimary:
    @pytest.mark.parametrize(
        "service",
        [
            primary_pipeline_service.get_lists_service,
            primary_pipeline_service.get_campaigns_service,
        ],
        ids=["lists", "campaigns"],
    )
    def test_service(self, service):
        res = service()
        print(res)
        assert res


class TestExport:
    @pytest.mark.parametrize(
        "service",
        [
            export_pipeline_service.get_members,
            export_pipeline_service.get_campaign_click_details,
            export_pipeline_service.get_campaign_open_details,
        ],
        ids=[
            "Members",
            "CampaignClickDetails",
            "CampaignOpenDetails",
        ],
    )
    def test_service(self, service):
        res = service("tmp/10543547/83277")
        res

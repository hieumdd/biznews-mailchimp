import pytest

from mailchimp import (
    operations,
    repo,
    primary_pipeline_service,
    secondary_pipeline_service,
    webhook_service,
)


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


class TestWebhook:
    @pytest.mark.parametrize(
        "url",
        [
            "https://storage.googleapis.com/biznews-tmp/members.tar.gz",
            "https://storage.googleapis.com/biznews-tmp/campaigns-email-activity-1.tar.gz",
            "https://storage.googleapis.com/biznews-tmp/campaigns-email-activity-2.tar.gz",
            "https://storage.googleapis.com/biznews-tmp/campaigns-click-details-1.tar.gz",
            "https://storage.googleapis.com/biznews-tmp/campaigns-click-details-2.tar.gz",
        ],
        ids=[
            operations.Operation.MEMBERS.value,
            operations.Operation.CAMPAIGN_EMAIL_ACTIVITY_1.value,
            operations.Operation.CAMPAIGN_EMAIL_ACTIVITY_2.value,
            operations.Operation.CAMPAIGN_CLICK_DETAILS_1.value,
            operations.Operation.CAMPAIGN_CLICK_DETAILS_2.value,
        ],
    )
    def test_service(self, url):
        res = webhook_service.webhook_service(url)
        assert res


class TestSecondary:
    @pytest.mark.parametrize(
        ["service", "response_fn"],
        [
            (
                secondary_pipeline_service.get_members_service,
                lambda: repo.client.lists.get_list_members_info("440df7dc51"),
            ),
            (
                secondary_pipeline_service.get_campaign_email_activity_2_service,
                lambda: repo.client.reports.get_email_activity_for_campaign(
                    "00012092fc"
                ),
            ),
            (
                secondary_pipeline_service.get_campaign_click_details_2_service,
                lambda: repo.client.reports.get_campaign_click_details("00012092fc"),
            ),
        ],
        ids=[
            operations.Operation.MEMBERS.value,
            operations.Operation.CAMPAIGN_EMAIL_ACTIVITY_2.value,
            operations.Operation.CAMPAIGN_CLICK_DETAILS_2.value,
        ],
    )
    def test_core_service(self, service, response_fn):
        responses = [response_fn()]
        res = service(responses)
        assert res

    @pytest.mark.parametrize(
        ["service", "response_fn"],
        [
            (
                secondary_pipeline_service.get_campaign_email_activity_1_service,
                lambda: repo.client.reports.get_email_activity_for_campaign(
                    "00012092fc"
                ),
            ),
            (
                secondary_pipeline_service.get_campaign_click_details_1_service,
                lambda: repo.client.reports.get_campaign_click_details("00012092fc"),
            ),
        ],
        ids=[
            operations.Operation.CAMPAIGN_EMAIL_ACTIVITY_1.value,
            operations.Operation.CAMPAIGN_CLICK_DETAILS_1.value,
        ],
    )
    def test_pagination_service(self, service, response_fn):
        responses = [response_fn()]
        res = service(responses)
        assert res

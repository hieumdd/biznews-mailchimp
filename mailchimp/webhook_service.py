from mailchimp import repo
from mailchimp.operations import Operation
from mailchimp.secondary_pipeline_service import (
    get_members_service,
    get_campaign_email_activity_1_service,
    get_campaign_email_activity_2_service,
    get_campaign_click_details_1_service,
    get_campaign_click_details_2_service,
)


def webhook_service(url: str):
    services = {
        Operation.MEMBERS.value: get_members_service,
        Operation.CAMPAIGN_EMAIL_ACTIVITY_1.value: get_campaign_email_activity_1_service,
        Operation.CAMPAIGN_EMAIL_ACTIVITY_2.value: get_campaign_email_activity_2_service,
        Operation.CAMPAIGN_CLICK_DETAILS_1.value: get_campaign_click_details_1_service,
        Operation.CAMPAIGN_CLICK_DETAILS_2.value: get_campaign_click_details_2_service,
    }

    batch_groups = repo.get_from_batch(url)

    return [services[operation_id](data) for operation_id, data in batch_groups]

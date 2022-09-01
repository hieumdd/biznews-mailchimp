from mailchimp.repo import get_from_batch
from mailchimp.operations import Operation


def webhook_service(url: str):
    services = {
        Operation.CAMPAIGN_OPEN_DETAILS_1.value: lambda x: x,
        Operation.CAMPAIGN_OPEN_DETAILS_2.value: lambda x: x,
        Operation.CAMPAIGN_CLICK_DETAILS_1.value: lambda x: x,
        Operation.CAMPAIGN_CLICK_DETAILS_2.value: lambda x: x,
    }

    batch_groups = get_from_batch(url)

    return [services[operation_id](data) for operation_id, data in batch_groups]

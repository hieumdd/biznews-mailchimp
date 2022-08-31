from compose import compose

from db import bigquery
from mailchimp.repo import (
    client,
    get_lists,
    get_archive,
    read_extractions,
    get_from_batch,
    create_paginated_batch_operation,
    create_batch_operation,
)
from mailchimp.operations import Operation


def secondary_pipeline_service(url: str):
    services = {
        Operation.CAMPAIGN_OPEN_DETAILS_1.value: lambda x: x,
        Operation.CAMPAIGN_OPEN_DETAILS_2.value: lambda x: x,
        Operation.CAMPAIGN_CLICK_DETAILS_1.value: lambda x: x,
        Operation.CAMPAIGN_CLICK_DETAILS_2.value: lambda x: x,
    }

    batch_groups = get_from_batch(url);

    return [services[operation_id](data) for operation_id, data in batch_groups]

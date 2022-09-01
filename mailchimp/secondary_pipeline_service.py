from typing import Callable, Any
from compose import compose

from db import bigquery
from mailchimp.repo import create_paginated_batch_operation
from mailchimp.operations import Operation


def get_core_service(
    parse_fn: Callable[[Any], list[dict]],
    transform_fn: Callable[[list[dict]], list[dict]],
    load_fn: Callable[[list[dict]], int],
):
    def _svc(responses):
        responses
        data = [i for j in [parse_fn(r) for r in responses] for i in j]
        data

        return compose(
            load_fn,
            transform_fn,
        )(data)

    return _svc


def get_pagination_service(
    operation_fn: Callable[[dict], tuple[str, int]],
    batch_operations: list[Callable[[list[dict]], Any]],
):
    def _svc(responses):
        operation_data = [operation_fn(res) for res in responses]

        return [operation(operation_data) for operation in batch_operations]

    return _svc


get_members_service = get_core_service(
    lambda res: res["members"],
    lambda rows: [
        {
            "id": row.get("id"),
            "list_id": row.get("list_id"),
            "web_id": row.get("web_id"),
            "unique_email_id": row.get("unique_email_id"),
            "email_address": row.get("email_address"),
            "status": row.get("status"),
            "source": row.get("source"),
            "last_changed": row.get("last_changed"),
        }
        for row in rows
    ],
    bigquery.load(
        "Members",
        [
            {"name": "id", "type": "STRING"},
            {"name": "list_id", "type": "STRING"},
            {"name": "web_id", "type": "STRING"},
            {"name": "unique_email_id", "type": "STRING"},
            {"name": "email_address", "type": "STRING"},
            {"name": "status", "type": "STRING"},
            {"name": "source", "type": "STRING"},
            {"name": "last_changed", "type": "TIMESTAMP"},
        ],
    ),
)

get_campaign_email_activity_1_service = get_pagination_service(
    lambda res: (res["campaign_id"], res["total_items"]),
    [
        create_paginated_batch_operation(
            Operation.CAMPAIGN_EMAIL_ACTIVITY_2.value,
            lambda item: f"/reports/{item[0]}/email-activity",
            lambda item, _: item[1],
        )
    ],
)

get_campaign_email_activity_2_service = get_core_service(
    lambda res: res["emails"],
    lambda rows: [
        {
            "campaign_id": row.get("campaign_id"),
            "list_id": row.get("list_id"),
            "list_is_active": row.get("list_is_active"),
            "email_id": row.get("email_id"),
            "email_address": row.get("email_address"),
            "activity": [
                {
                    "action": activity.get("action"),
                    "timestamp": activity.get("timestamp"),
                    "ip": activity.get("ip"),
                }
                for activity in row["activity"]
            ]
            if row.get("activity")
            else [],
        }
        for row in rows
    ],
    bigquery.load(
        "CampaignEmailActivity",
        [
            {"name": "campaign_id", "type": "STRING"},
            {"name": "list_id", "type": "STRING"},
            {"name": "list_is_active", "type": "BOOLEAN"},
            {"name": "email_id", "type": "STRING"},
            {"name": "email_address", "type": "STRING"},
            {
                "name": "activity",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {"name": "action", "type": "STRING"},
                    {"name": "timestamp", "type": "TIMESTAMP"},
                    {"name": "ip", "type": "STRING"},
                ],
            },
        ],
    ),
)

get_campaign_click_details_1_service = get_pagination_service(
    lambda res: (res["campaign_id"], res["total_items"]),
    [
        create_paginated_batch_operation(
            Operation.CAMPAIGN_CLICK_DETAILS_2.value,
            lambda item: f"/reports/{item[0]}/click-details",
            lambda item, _: item[1],
        )
    ],
)

get_campaign_click_details_2_service = get_core_service(
    lambda res: res["urls_clicked"],
    lambda rows: [
        {
            "id": row.get("id"),
            "url": row.get("url"),
            "total_clicks": row.get("total_clicks"),
            "click_percentage": row.get("click_percentage"),
            "unique_clicks": row.get("unique_clicks"),
            "unique_click_percentage": row.get("unique_click_percentage"),
            "last_click": row.get("last_click"),
            "campaign_id": row.get("campaign_id"),
        }
        for row in rows
    ],
    bigquery.load(
        "CampaignClickDetails",
        [
            {"name": "id", "type": "STRING"},
            {"name": "url", "type": "STRING"},
            {"name": "total_clicks", "type": "NUMERIC"},
            {"name": "click_percentage", "type": "BIGNUMERIC"},
            {"name": "unique_clicks", "type": "NUMERIC"},
            {"name": "unique_click_percentage", "type": "BIGNUMERIC"},
            {"name": "last_click", "type": "TIMESTAMP"},
            {"name": "campaign_id", "type": "STRING"},
        ],
    ),
)

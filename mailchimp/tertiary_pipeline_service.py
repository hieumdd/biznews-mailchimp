from typing import Optional, Callable
from compose import compose

from db import bigquery
from mailchimp.repo import (
    client,
    get_lists,
    get_archive,
    read_extractions,
    create_paginated_batch_operation,
    create_batch_operation,
)
from mailchimp.operations import Operation
from mailchimp.utils import round_float


def get_members_service(responses):
    data = [r["members"] for r in responses]

    return compose(
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
    )(data)


def get_open_details_1_service(responses):
    operation_data = [
        (response["campaign_id"], response["total_items"]) for response in responses
    ]

    return create_paginated_batch_operation(
        "members",
        lambda item: f"/reports/{item[0]}/open-details",
        lambda item, _: item[1],
    )(operation_data)


def get_open_details_2_service(responses):
    data = [r["members"] for r in responses]

    return compose(
        bigquery.load(
            "CampaignOpenDetails",
            [
                {"name": "campaign_id", "type": "STRING"},
                {"name": "list_id", "type": "STRING"},
                {"name": "list_is_active", "type": "BOOLEAN"},
                {"name": "contact_status", "type": "STRING"},
                {"name": "email_id", "type": "STRING"},
                {"name": "email_address", "type": "STRING"},
                {"name": "opens_count", "type": "NUMERIC"},
            ],
        ),
        lambda rows: [
            {
                "campaign_id": row.get("campaign_id"),
                "list_id": row.get("list_id"),
                "list_is_active": row.get("list_is_active"),
                "contact_status": row.get("contact_status"),
                "email_id": row.get("email_id"),
                "email_address": row.get("email_address"),
                "opens_count": row.get("opens_count"),
            }
            for row in rows
        ],
    )(data)


def get_click_details_1_service(responses):
    operation_data = [
        i
        for j in [
            (item["campaign_id"], item["id"])
            for item in [response["urls_clicked"] for response in responses]
        ]
        for i in j
    ]

    return create_batch_operation(
        Operation.CAMPAIGN_OPEN_DETAILS_2.value,
        lambda item: f"/reports/{item[0]}/click-details/{item[1]}",
    )(operation_data)


def tertiary_pipeline_service(
    parse_fn: Callable,
    batch_operation_options: list[Callable] = [],
    transform_fn: Optional[Callable] = None,
    load_fn: Optional[Callable] = None,
):
    def _svc(responses: list[dict]):
        data = [i for j in [parse_fn(r) for r in responses] for i in j]

        operations = [
            operation(data, response) for operation in batch_operation_options
        ]

        if transform_fn and load_fn:
            load = compose(
                load_fn,
                transform_fn,
            )(data)

            return {"operations": len(operations), "load": load}

        return len(operations)

    return _svc


get_click_details_1_service = tertiary_pipeline_service(
    [
        create_paginated_batch_operation(
            "members",
            lambda item: f"/reports/{item['campaign_id']}/open-details",
            lambda item: item["total_items"],
        )
    ]
)

get_click_details_2_service = tertiary_pipeline_service(
    transform_fn=lambda rows: [
        {
            "campaign_id": row.get("campaign_id"),
            "list_id": row.get("list_id"),
            "list_is_active": row.get("list_is_active"),
            "contact_status": row.get("contact_status"),
            "email_id": row.get("email_id"),
            "email_address": row.get("email_address"),
            "opens_count": row.get("opens_count"),
        }
        for row in rows
    ],
    load_fn=bigquery.load(
        "CampaignOpenDetails",
        [
            {"name": "campaign_id", "type": "STRING"},
            {"name": "list_id", "type": "STRING"},
            {"name": "list_is_active", "type": "BOOLEAN"},
            {"name": "contact_status", "type": "STRING"},
            {"name": "email_id", "type": "STRING"},
            {"name": "email_address", "type": "STRING"},
            {"name": "opens_count", "type": "NUMERIC"},
        ],
    ),
)


get_lists_service = tertiary_pipeline_service(
    client.lists.get_all_lists,
    lambda x: x["lists"],
    [
        create_paginated_batch_operation(
            "members",
            lambda item: f"/lists/{item['id']}/members",
            lambda item: item["stats"]["member_count"],
        )
    ],
    lambda rows: [
        {
            "id": item.get("id"),
            "name": item.get("name"),
        }
        for item in rows
    ],
    "Lists",
    [
        {"name": "id", "type": "STRING"},
        {"name": "name", "type": "STRING"},
    ],
)

get_campaigns_service = tertiary_pipeline_service(
    client.campaigns.list,
    lambda x: x["campaigns"],
    [
        create_batch_operation(
            "open-details-1",
            lambda item: f"/reports/{item['id']}/open-details",
        ),
        create_batch_operation(
            "click-details-1",
            lambda item: f"/reports/{item['id']}/click-details",
        ),
    ],
    lambda rows: [
        {
            "id": item.get("id"),
            "create_time": item.get("create_time"),
            "status": item.get("status"),
            "web_id": item.get("web_id"),
            "type": item.get("type"),
            "emails_sent": item.get("emails_sent"),
            "report_summary": {
                "opens": item["report_summary"].get("opens"),
                "unique_opens": item["report_summary"].get("unique_opens"),
                "open_rate": round_float(item["report_summary"].get("open_rate")),
                "clicks": item["report_summary"].get("clicks"),
                "subscriber_clicks": item["report_summary"].get("subscriber_clicks"),
                "click_rate": round_float(item["report_summary"].get("click_rate")),
                "ecommerce": {
                    "total_orders": item["report_summary"]["ecommerce"].get(
                        "total_orders"
                    ),
                    "total_spent": item["report_summary"]["ecommerce"].get(
                        "total_spent"
                    ),
                    "total_revenue": item["report_summary"]["ecommerce"].get(
                        "total_revenue"
                    ),
                }
                if item["report_summary"].get("ecommerce")
                else {},
            }
            if item.get("report_summary")
            else {},
        }
        for item in rows
    ],
    "Campaigns",
    [
        {"name": "id", "type": "STRING"},
        {"name": "create_time", "type": "TIMESTAMP"},
        {"name": "status", "type": "STRING"},
        {"name": "web_id", "type": "NUMERIC"},
        {"name": "type", "type": "STRING"},
        {"name": "emails_sent", "type": "NUMERIC"},
        {
            "name": "report_summary",
            "type": "RECORD",
            "fields": [
                {"name": "opens", "type": "NUMERIC"},
                {"name": "unique_opens", "type": "NUMERIC"},
                {"name": "open_rate", "type": "NUMERIC"},
                {"name": "clicks", "type": "NUMERIC"},
                {"name": "subscriber_clicks", "type": "NUMERIC"},
                {"name": "click_rate", "type": "NUMERIC"},
                {
                    "name": "ecommerce",
                    "type": "RECORD",
                    "fields": [
                        {"name": "total_orders", "type": "NUMERIC"},
                        {"name": "total_spent", "type": "NUMERIC"},
                        {"name": "total_revenue", "type": "NUMERIC"},
                    ],
                },
            ],
        },
    ],
)

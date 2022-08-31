from compose import compose

from db import bigquery
from mailchimp.repo import (
    client,
    get_lists,
    create_paginated_batch_operation,
    create_batch_operation,
)
from mailchimp.operations import Operation
from mailchimp.utils import round_float


def primary_pipeline_service(
    method,
    parse_fn,
    batch_operation_options,
    transform_fn,
    table,
    schema,
):
    def _svc():
        data = get_lists(method, parse_fn)

        [operation(data) for operation in batch_operation_options]

        return compose(
            bigquery.load(table, schema),
            transform_fn,
        )(data)

    return _svc


get_lists_service = primary_pipeline_service(
    client.lists.get_all_lists,
    lambda x: x["lists"],
    [
        create_paginated_batch_operation(
            Operation.MEMBERS.value,
            lambda item: f"/lists/{item['id']}/members",
            lambda item, _: item["stats"]["member_count"],
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

get_campaigns_service = primary_pipeline_service(
    client.campaigns.list,
    lambda x: x["campaigns"],
    [
        create_batch_operation(
            Operation.CAMPAIGN_OPEN_DETAILS_1.value,
            lambda item: f"/reports/{item['id']}/open-details",
        ),
        create_batch_operation(
            Operation.CAMPAIGN_CLICK_DETAILS_1.value,
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

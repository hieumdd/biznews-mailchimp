from typing import Callable

from compose import compose

from db import bigquery
from mailchimp import repo


def primary_pipeline_service(
    method: Callable,
    parse_fn: Callable[[dict], list[dict]],
    transform_fn: Callable[[list[dict]], list[dict]],
    load_fn: Callable[[list[dict]], int],
):
    def _svc():
        return compose(
            load_fn,
            transform_fn,
            repo.get_lists(method, parse_fn),
        )()

    return _svc


get_lists_service = primary_pipeline_service(
    repo.client.lists.get_all_lists,
    lambda x: x["lists"],
    lambda rows: [
        {
            "id": item.get("id"),
            "name": item.get("name"),
        }
        for item in rows
    ],
    bigquery.load(
        "Lists",
        [
            {"name": "id", "type": "STRING"},
            {"name": "name", "type": "STRING"},
        ],
    ),
)

get_campaigns_service = primary_pipeline_service(
    repo.client.campaigns.list,
    lambda x: x["campaigns"],
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
                "open_rate": item["report_summary"].get("open_rate"),
                "clicks": item["report_summary"].get("clicks"),
                "subscriber_clicks": item["report_summary"].get("subscriber_clicks"),
                "click_rate": item["report_summary"].get("click_rate"),
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
    bigquery.load(
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
                    {"name": "open_rate", "type": "BIGNUMERIC"},
                    {"name": "clicks", "type": "NUMERIC"},
                    {"name": "subscriber_clicks", "type": "NUMERIC"},
                    {"name": "click_rate", "type": "BIGNUMERIC"},
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
    ),
)

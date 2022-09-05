import os
import csv

from db import bigquery


def read_campaign_csv(fields: list[str], data_path: str, filename: str) -> list[dict]:
    filepath = os.path.join(data_path, filename)
    campaign_web_id = filename.split("_")[0]

    with open(filepath, "r") as f:
        next(f)
        csv_reader = csv.DictReader(f, fields)
        data = [
            {
                **row,  # type: ignore
                "campaign_web_id": int(campaign_web_id),
            }
            for row in csv_reader
        ]

    return data


def get_export_data(
    path_to_files: str,
    fields: list[str],
    table: str,
    schema: list[dict],
):
    def _get(path: str) -> int:
        data_path = os.path.join(path, path_to_files)

        data = sum(  # type: ignore
            [
                read_campaign_csv(fields, data_path, filename)
                for filename in os.listdir(os.path.join(data_path))
            ],
            [],
        )

        return bigquery.load(table, schema)(data)

    return _get


get_campaign_click_details = get_export_data(
    "granular_activity/clicks",
    ["timestamp", "email", "url"],
    "Export_CampaignClickDetails",
    [
        {"name": "timestamp", "type": "TIMESTAMP"},
        {"name": "email", "type": "STRING"},
        {"name": "url", "type": "STRING"},
        {"name": "campaign_web_id", "type": "NUMERIC"},
    ],
)

get_campaign_open_details = get_export_data(
    "granular_activity/opens",
    ["timestamp", "email"],
    "Export_CampaignOpenDetails",
    [
        {"name": "timestamp", "type": "TIMESTAMP"},
        {"name": "email", "type": "STRING"},
        {"name": "campaign_web_id", "type": "NUMERIC"},
    ],
)

from typing import Callable
import os
import csv

import numpy as np
import pandas as pd
from compose import compose

from db import bigquery


def read_campaign_csv(
    metadata_fn: Callable[[str], dict],
    data_path: str,
    filename: str,
) -> list[dict]:
    filepath = os.path.join(data_path, filename)

    # with open(filepath, "r") as f:
    #     csv_reader = csv.DictReader(f)
    #     data = [
    #         {
    #             **row,  # type: ignore
    #             **metadata_fn(filename),
    #         }
    #         for row in csv_reader
    #     ]

    # return data

    df = pd.read_csv(filepath)

    data = df.fillna(value=np.nan).replace({np.nan: None}).to_dict("records")

    return [
        {
            **row,
            **metadata_fn(filename),
        }
        for row in data
    ]


def get_export_data(
    path_to_files: str,
    metadata_fn: Callable[[str], dict],
    transform_fn: Callable[[list[dict]], list[dict]],
    table: str,
    schema: list[dict],
):
    def _get(path: str) -> int:
        data_path = os.path.join(path, path_to_files)

        data = sum(  # type: ignore
            [
                read_campaign_csv(metadata_fn, data_path, filename)
                for filename in os.listdir(os.path.join(data_path))
            ],
            [],
        )

        return compose(
            bigquery.load(table, schema),
            transform_fn,
        )(data)

    return _get


get_members = get_export_data(
    "lists/members",
    lambda filename: {"list_web_id": int(filename.split("_")[1])},
    lambda rows: [
        {
            "email_address": row.get("Email Address"),
            "first_name": row.get("First Name"),
            "last_name": row.get("Last Name"),
            "status": row.get("Status"),
            "member_rating": row.get("MEMBER_RATING"),
            "optin_time": row.get("OPTIN_TIME"),
            "optin_ip": row.get("OPTIN_IP"),
            "confirm_time": row.get("CONFIRM_TIME"),
            "confirm_ip": row.get("CONFIRM_IP"),
            "latitude": row.get("LATITUDE"),
            "longitude": row.get("LONGITUDE"),
            "gmtoff": row.get("GMTOFF"),
            "dstoff": row.get("DSTOFF"),
            "timezone": row.get("TIMEZONE"),
            "cc": row.get("CC"),
            "region": row.get("REGION"),
            "last_changed": row.get("LAST_CHANGED"),
            "leid": row.get("LEID"),
            "euid": row.get("EUID"),
            "notes": row.get("NOTES"),
            "tags": row.get("TAGS"),
            "list_web_id": row["list_web_id"],
        }
        for row in rows
    ],
    "Export_Members",
    [
        {"name": "email_address", "type": "STRING"},
        {"name": "first_name", "type": "STRING"},
        {"name": "last_name", "type": "STRING"},
        {"name": "status", "type": "STRING"},
        {"name": "member_rating", "type": "NUMERIC"},
        {"name": "optin_time", "type": "TIMESTAMP"},
        {"name": "optin_ip", "type": "STRING"},
        {"name": "confirm_time", "type": "TIMESTAMP"},
        {"name": "confirm_ip", "type": "STRING"},
        {"name": "latitude", "type": "STRING"},
        {"name": "longitude", "type": "STRING"},
        {"name": "gmtoff", "type": "STRING"},
        {"name": "dstoff", "type": "STRING"},
        {"name": "timezone", "type": "STRING"},
        {"name": "cc", "type": "STRING"},
        {"name": "region", "type": "STRING"},
        {"name": "last_changed", "type": "TIMESTAMP"},
        {"name": "leid", "type": "NUMERIC"},
        {"name": "euid", "type": "STRING"},
        {"name": "notes", "type": "STRING"},
        {"name": "tags", "type": "STRING"},
        {"name": "list_web_id", "type": "NUMERIC"},
    ],
)

get_campaign_click_details = get_export_data(
    "granular_activity/clicks",
    lambda filename: {"campaign_web_id": int(filename.split("_")[0])},
    lambda rows: [
        {
            "timestamp": row.get("Timestamp"),
            "email": row.get("Email"),
            "url": row.get("Url"),
            "campaign_web_id": row["campaign_web_id"],
        }
        for row in rows
    ],
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
    lambda filename: {"campaign_web_id": int(filename.split("_")[0])},
    lambda rows: [
        {
            "timestamp": row.get("Timestamp"),
            "email": row.get("Email"),
            "campaign_web_id": row["campaign_web_id"],
        }
        for row in rows
    ],
    "Export_CampaignOpenDetails",
    [
        {"name": "timestamp", "type": "TIMESTAMP"},
        {"name": "email", "type": "STRING"},
        {"name": "campaign_web_id", "type": "NUMERIC"},
    ],
)

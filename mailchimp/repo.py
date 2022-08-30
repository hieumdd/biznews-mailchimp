from typing import Optional, Callable
import os
import math
import json
import tarfile
from uuid import uuid4

import httpx
import mailchimp_marketing as MailchimpMarketing

client = MailchimpMarketing.Client()
client.set_config({"api_key": os.getenv("MAILCHIMP_API_KEY"), "server": "us5"})

METHOD = "GET"
COUNT = 1000


def get_lists(method, parse_fn, offset=0):
    res = method(count=COUNT, offset=offset)
    data = parse_fn(res)
    return (
        data if len(data) == 0 else data + get_lists(method, parse_fn, offset + COUNT)
    )


def create_paginated_batch_operation(operation_id, path_fn, count_fn):
    def _create(lists):
        def create_batch(path, count: int):
            pages = math.ceil(count / COUNT)
            return [
                {
                    "method": METHOD,
                    "path": path,
                    "params": {
                        "count": COUNT,
                        "offset": i,
                    },
                }
                for i in range(pages)
            ]

        operations = [
            i
            for j in [create_batch(path_fn(item), count_fn(item)) for item in lists]
            for i in j
        ]

        return client.batches.start(
            {"operations": operations[:1], "operation_id": operation_id}
        )

    return _create


def create_batch_operation(operation_id, path_fn):
    def _create(lists):
        operations = [
            {
                "method": METHOD,
                "path": path_fn(item),
                "params": {"count": 1},
            }
            for item in lists
        ]

        return client.batches.start(
            {"operations": operations[:1], "operation_id": operation_id}
        )

    return _create


def get_archive(url: str) -> str:
    dirpath = f"tmp/{uuid4()}"
    filepath = f"{dirpath}.tar.gz"

    res = httpx.get(url)

    with open(filepath, "wb") as f:
        f.write(res.content)

    with tarfile.open(filepath) as t:
        t.extractall(dirpath)

    return dirpath


def read_extractions(dirpath: str):
    def parse_response(path: str):
        with open(path, "r") as f:
            res = json.load(f)
            return res

    def parse_data(response):
        if response["status_code"] != 200:
            return []

        return (lambda x: x["members"])(json.loads(response["response"]))

    data = [
        parse_data(i)
        for j in [parse_response(f"{dirpath}/{path}") for path in os.listdir(dirpath)]
        for i in j
    ]

    return [i for j in data for i in j]

from typing import Any
import math
from mailchimp.repo import client


def create_page_batch(method, path, count: int):
    pages = math.ceil(count / 1000)
    return [
        {
            "method": method,
            "path": path,
            "params": {
                "count": 1000,
                "offset": i,
            },
        }
        for i in range(pages)
    ]


def create_member_batch():
    def get_lists():
        return client.lists.get_all_lists(count=1000)

    lists = get_lists()

    operations = sum(
        [
            create_page_batch(
                "GET",
                f"/lists/{list_['id']}/members",
                list_["stat"]["member_count"],
            )
            for list_ in lists
        ]
    )

    client.batches.start({"operations": operations})

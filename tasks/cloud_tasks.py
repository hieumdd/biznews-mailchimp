from typing import Callable, Any
import os
import json
import uuid

from google.cloud import tasks_v2
from google import auth


_, PROJECT_ID = auth.default()


def create_tasks(
    queue: str,
    payloads: list[dict[str, Any]],
    name_fn: Callable[[dict[str, Any]], str],
) -> int:
    with tasks_v2.CloudTasksClient() as client:
        task_path = (PROJECT_ID, "us-central1", queue)
        parent = client.queue_path(*task_path)
        tasks = [
            {
                "name": client.task_path(
                    *task_path,
                    task=f"{name_fn(payload)}-{uuid.uuid4()}",
                ),
                "http_request": {
                    "http_method": tasks_v2.HttpMethod.POST,
                    "url": os.getenv("PUBLIC_URL"),
                    "oidc_token": {
                        "service_account_email": os.getenv("GCP_SA"),
                    },
                    "headers": {
                        "Content-type": "application/json",
                    },
                    "body": json.dumps(payload).encode(),
                },
            }
            for payload in payloads
        ]
        return len(
            [
                client.create_task(
                    request={
                        "parent": parent,
                        "task": task,
                    }
                )
                for task in tasks
            ]
        )

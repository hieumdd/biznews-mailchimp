from typing import Callable, Any
import os
import json
from uuid import uuid4
from datetime import datetime, timedelta

from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2
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

        schedule_time = timestamp_pb2.Timestamp()
        schedule_time.FromDatetime(datetime.utcnow() + timedelta(minutes=30))

        tasks = [
            {
                "name": client.task_path(
                    *task_path,
                    task=f"{name_fn(payload)}-{uuid4()}",
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
                "schedule_time": schedule_time,
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

from typing import Any

from mailchimp import coffeehr_service
from mailchimp.pipeline import pipelines


def main(request):
    data: dict[str, Any] = request.get_json()
    sector: str = request.args.get("sector")
    print(data, request.args)

    if request.method == "OPTIONS":
        # Allows GET requests from any origin with the Content-Type
        # header and caches preflight response for an 3600s
        headers = {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Max-Age": "3600",
        }

        return ("", 204, headers)

    if data:
        if "tasks" in data:
            response = coffeehr_service.create_tasks_service()
        elif "table" in data:
            response = coffeehr_service.pipeline_service(pipelines[data["table"]])
    elif sector:
        response = coffeehr_service.employee_service(sector)
    else:
        raise ValueError(data)

    print(response)
    return (
        response,
        200,
        {"Access-Control-Allow-Methods": "POST", "Access-Control-Allow-Origin": "*"},
    )

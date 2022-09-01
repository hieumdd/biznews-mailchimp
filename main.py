import json

from mailchimp import primary_pipeline_service, webhook_service


def main(request):
    print(request)

    if "webhook" in request.path:
        data = json.loads(request.form["data"])
        response = webhook_service.webhook_service(data["response_body_url"])
    else:
        response = [
            primary_pipeline_service.get_lists_service(),
            primary_pipeline_service.get_campaigns_service(),
        ]

    print(response)

    return {"results": response}

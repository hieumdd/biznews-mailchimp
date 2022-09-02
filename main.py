import json

from flask import Flask, Request, request

from mailchimp import primary_pipeline_service, webhook_service

app = Flask("internal")


@app.route("/")
def pipeline_controller():
    return [
        primary_pipeline_service.get_lists_service(),
        primary_pipeline_service.get_campaigns_service(),
    ]


@app.route("/webhook")
def webhook_controller():
    data = json.loads(request.form["data"])
    return webhook_service.webhook_service(data["response_body_url"])


def main(request: Request):
    print(request)

    ctx = app.test_request_context(path=request.path, data=request.data)
    ctx.push()

    response = app.full_dispatch_request()

    ctx.pop()

    print(response)

    return {"response": response}

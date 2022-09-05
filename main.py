from mailchimp import primary_service, export_service


def main(request):
    print(request)

    data: dict = request.get_json()

    if "export_id" in data:
        response = export_service.export_service(data["export_id"])
    else:
        response = primary_service.primary_service()

    print(response)

    return {"response": response}

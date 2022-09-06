from mailchimp import primary_service, export_service


def test_primary_service():
    res = primary_service.primary_service()
    print(res)
    assert res


def test_export_service():
    export_id = 4
    res = export_service.export_service(export_id)
    print(res)
    assert res

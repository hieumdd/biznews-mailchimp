from mailchimp.pipeline import primary_pipeline_service
from mailchimp import repo, export_service


def primary_service():
    results = [
        service()
        for service in [
            primary_pipeline_service.get_lists,
            primary_pipeline_service.get_campaigns,
        ]
    ]

    export_id = repo.create_export()
    tasks = export_service.create_export_task(export_id)

    return [results, tasks]

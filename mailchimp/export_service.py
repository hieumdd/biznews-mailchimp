from mailchimp.pipeline import export_pipeline_service
from tasks import cloud_tasks
from mailchimp import repo


def create_export_task(export_id: int):
    return cloud_tasks.create_tasks(
        "mailchimp-export",
        [{"export_id": export_id}],
        lambda x: x["export_id"],
    )


def export_service(export_id: int):
    download_url = repo.get_export_download_url(export_id)

    if download_url:
        extract_path = repo.get_archive(download_url)

        response = [
            service(extract_path)
            for service in [
                export_pipeline_service.get_campaign_click_details,
                export_pipeline_service.get_campaign_open_details,
            ]
        ]
    else:
        response = create_export_task(export_id)

    return response

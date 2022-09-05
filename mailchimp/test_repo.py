from mailchimp import repo


def test_get_export_info():
    export_id = 4
    url = repo.get_export_download_url(export_id)
    res = repo.get_archive(url)
    res

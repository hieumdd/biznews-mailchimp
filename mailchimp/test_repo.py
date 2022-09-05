from mailchimp import repo

def test_get_export_info():
    export_id = 4
    url = repo.get_export_download_url(export_id)
    res = repo.get_archive(url)
    res

def test_read_extraction():
    dirpath = f"tmp/df6ce4d8-9b10-4a7d-99f9-8fb0e8ea7cfc"
    res = repo.read_extractions(dirpath)
    res

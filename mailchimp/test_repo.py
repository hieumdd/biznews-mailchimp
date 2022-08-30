from mailchimp import repo


def test_read_extraction():
    dirpath = f"tmp/df6ce4d8-9b10-4a7d-99f9-8fb0e8ea7cfc"
    res = repo.read_extractions(dirpath)
    res

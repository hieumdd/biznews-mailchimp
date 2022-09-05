from typing import Callable, Optional
import os
import zipfile

import httpx
import mailchimp_marketing as MailchimpMarketing

client = MailchimpMarketing.Client()
client.set_config({"api_key": os.getenv("MAILCHIMP_API_KEY"), "server": "us5"})

METHOD = "GET"
COUNT = 1000


def get_lists(
    method: Callable,
    parse_fn: Callable[[dict], list[dict]],
    offset: int = 0,
) -> list[dict]:
    res = method(count=COUNT, offset=offset)
    data = parse_fn(res)
    return (
        data if len(data) == 0 else data + get_lists(method, parse_fn, offset + COUNT)
    )


def create_export() -> str:
    return client.accountExports.create_account_export(
        {"include_stages": ["audiences", "campaigns", "reports"]}
    ).get("export_id")


def get_export_download_url(export_id: str) -> Optional[str]:
    return client.accountExport.get_account_exports(export_id).get("download_url")


def get_archive(url: str) -> str:
    extract_path = "tmp" if os.getenv("PYTHON_ENV") == "dev" else "/tmp"

    res = httpx.get(url)

    filepath = res.request.url.path.split("/").pop()
    dirpath = f"{extract_path}/{filepath}"

    with open(dirpath, "wb") as f:
        f.write(res.content)

    with zipfile.ZipFile(dirpath) as z:
        z.extractall(extract_path)

    data_path = filepath.replace(".zip", "").split("-")

    return os.path.join(extract_path, *data_path)

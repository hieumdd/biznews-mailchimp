from mailchimp.pipeline.interface import Pipeline

pipeline = Pipeline(
    name="Departments",
    get_options=("GET", "SYN_EL_PHONGBAN"),
    transform=lambda rows: [
        {
            "IsDeleted": row.get("IsDeleted"),
            "LastUpdate": row.get("LastUpdate"),
            "MaPhongBan": row.get("MaPhongBan"),
            "MaPhongBanCha": row.get("MaPhongBanCha"),
            "TenPhongBan": row.get("TenPhongBan"),
            "TenPhongBanTA": row.get("TenPhongBanTA"),
        }
        for row in rows
    ],
    schema=[
        {"name": "IsDeleted", "type": "BOOLEAN"},
        {"name": "LastUpdate", "type": "TIMESTAMP"},
        {"name": "MaPhongBan", "type": "NUMERIC"},
        {"name": "MaPhongBanCha", "type": "NUMERIC"},
        {"name": "TenPhongBan", "type": "STRING"},
        {"name": "TenPhongBanTA", "type": "STRING"},
    ],
)

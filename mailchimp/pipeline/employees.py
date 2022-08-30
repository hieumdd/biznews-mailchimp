from mailchimp.pipeline.interface import Pipeline

pipeline = Pipeline(
    name="Employees",
    get_options=("POST", "SYN_EL_NHANVIEN"),
    transform=lambda rows: [
        {
            "MaNhanVien": row.get("MaNhanVien"),
            "Ho": row.get("Ho"),
            "HoTen": row.get("HoTen"),
            "MaPhongBan": row.get("MaPhongBan"),
            "MaChucVu": row.get("MaChucVu"),
            "TenChucVu": row.get("TenChucVu"),
            "MaChucDanh": row.get("MaChucDanh"),
            "TenChucDanh": row.get("TenChucDanh"),
            "LoaiTaiKhoan": row.get("LoaiTaiKhoan"),
            "NgayBatDauLam": row.get("NgayBatDauLam"),
            "ViTriGioCong": row.get("ViTriGioCong"),
            "MaNhanVienQuanLy": row.get("MaNhanVienQuanLy"),
            "Email": row.get("Email"),
            "DienThoai": row.get("DienThoai"),
            "TrangThai": row.get("TrangThai"),
            "TrangThaiLaoDong": row.get("TrangThaiLaoDong"),
            "NgayNghiViec": row.get("NgayNghiViec"),
            "LastUpdate": row.get("LastUpdate"),
        }
        for row in rows
    ],
    schema=[
        {"name": "MaNhanVien", "type": "STRING"},
        {"name": "Ho", "type": "STRING"},
        {"name": "HoTen", "type": "STRING"},
        {"name": "MaPhongBan", "type": "NUMERIC"},
        {"name": "MaChucVu", "type": "NUMERIC"},
        {"name": "TenChucVu", "type": "STRING"},
        {"name": "MaChucDanh", "type": "NUMERIC"},
        {"name": "TenChucDanh", "type": "STRING"},
        {"name": "LoaiTaiKhoan", "type": "BOOLEAN"},
        {"name": "NgayBatDauLam", "type": "TIMESTAMP"},
        {"name": "ViTriGioCong", "type": "STRING"},
        {"name": "MaNhanVienQuanLy", "type": "STRING"},
        {"name": "Email", "type": "STRING"},
        {"name": "DienThoai", "type": "STRING"},
        {"name": "TrangThai", "type": "BOOLEAN"},
        {"name": "TrangThaiLaoDong", "type": "STRING"},
        {"name": "NgayNghiViec", "type": "TIMESTAMP"},
        {"name": "LastUpdate", "type": "TIMESTAMP"},
    ],
)

from process_data import zipfile_to_csv
from pathlib import Path

def test_zipfile_to_csv():
    zip_path = Path("data/MSIM2.zip")
    csv_path = Path("data/csv")
    csv_files = zipfile_to_csv(zip_path, csv_path)
    assert len(csv_files) == 1
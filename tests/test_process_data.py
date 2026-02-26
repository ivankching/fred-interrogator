from process_data import zipfile_to_csv, get_csv_schema, infer_type
import duckdb
from pathlib import Path

def test_zipfile_to_csv():
    zip_path = Path("tests/data/MSIM2.zip")
    csv_path = Path("tests/data/csv")
    csv_files = zipfile_to_csv(zip_path, csv_path)
    assert len(csv_files) == 1

# TODO: Test get_csv_schema
def test_get_csv_schema():
    schema = get_csv_schema(Path("tests/obs._by_real-time_period_MSIM2.csv"))
    columns = ["period_start_date", "MSIM2", "realtime_start_date", "realtime_end_date"]
    types = {"period_start_date": "date", "MSIM2": "float", "realtime_start_date": "date", "realtime_end_date": "empty"}
    sample_size = 100
    
    assert schema['columns'] == columns
    assert schema['types'] == types
    assert schema['sample_size'] == sample_size

def test_infer_type():
    integer_values = ['1', '2', '3']
    float_values = ['1.0', '2.0', '3.0']
    string_valeus = ['a', 'b', 'c']
    date_values = ['2020-01-01', '2020-02-01', '2020-03-01']

    assert infer_type(integer_values) == 'integer'
    assert infer_type(float_values) == 'float'
    assert infer_type(string_valeus) == 'string'
    assert infer_type(date_values) == 'date'

def test_db_relation_from_csv():
    csv_path = Path('tests/obs._by_real-time_period_MSIM2.csv')
    relation = duckdb.read_csv(csv_path)
    assert relation

    results = duckdb.sql("SELECT COUNT(*) FROM relation").fetchall()
    print(results)
    assert(results == [(660,)])
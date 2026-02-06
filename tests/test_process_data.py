from process_data import zipfile_to_csv, get_csv_schema, infer_type
from pathlib import Path

def test_zipfile_to_csv():
    zip_path = Path("data/MSIM2.zip")
    csv_path = Path("data/csv")
    csv_files = zipfile_to_csv(zip_path, csv_path)
    assert len(csv_files) == 1

# TODO: Test get_csv_schema
def test_get_csv_schema():
    schema = get_csv_schema(Path('data/csv/obs._by_real-time_period.csv'))
    print(f"Columns: {schema['columns']}")
    print(f"\nTypes:")
    for col, dtype in schema['types'].items():
        print(f"  {col}: {dtype}")

def test_infer_type():
    integer_values = ['1', '2', '3']
    float_values = ['1.0', '2.0', '3.0']
    string_valeus = ['a', 'b', 'c']
    date_values = ['2020-01-01', '2020-02-01', '2020-03-01']

    assert infer_type(integer_values) == 'integer'
    assert infer_type(float_values) == 'float'
    assert infer_type(string_valeus) == 'string'
    assert infer_type(date_values) == 'date'
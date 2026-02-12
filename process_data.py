import zipfile
import logfire
from pathlib import Path
import csv
from datetime import datetime
import random
import duckdb

logfire.configure(send_to_logfire=True)

def zipfile_to_csv(zip_path: Path, csv_path: Path = Path("data/csv")) -> list[Path]:
    """
    Unzip a zip file containing CSV files and extract them to a specified path.

    Parameters
    ----------
    zip_path : str
        Path to the zip file containing CSV files

    csv_path : str, optional
        Path to save the extracted CSV files to. Defaults to "data/csv"

    Returns
    -------
    list[str]
        List of paths to the extracted CSV files
    """
    unzipped_files = []
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Get all filenames
            all_files = zip_ref.namelist()
        
            # Filter for CSV files
            csv_files = [f for f in all_files if f.endswith('.csv')]
            
            print(f'Found {len(csv_files)} CSV files:')
            for filename in csv_files:
                print(f'  - {filename}')
                
            # Extract all CSV files
            for csv_file in csv_files:
                zip_ref.extract(csv_file, csv_path)

            unzipped_files = [csv_path / f for f in csv_files]

    except Exception as e:
        logfire.error(f"Error extracting zip file: {e}")
    logfire.info(f"CSV files extracted: {unzipped_files}")
    return unzipped_files


def get_csv_schema(filepath):
    """
    Get the schema of a CSV file.

    Parameters
    ----------
    filepath : Path
        Path to the CSV file to get the schema of

    Returns
    -------
    dict
        A dictionary containing the schema of the CSV file.
        The dictionary will have three keys: 'columns', 'types', and 'sample_size'.
        The value for 'columns' will be a list of column names.
        The value for 'types' will be a dictionary where the keys are column names and the values are the inferred types of the columns.
        The value for 'sample_size' will be the number of rows that were sampled to infer the types of the columns.
    """
    with open(filepath, 'r') as f:
        reader = csv.reader(f)
        
        # Get headers
        headers = next(reader)
        
        # Randomly sample 100 rows to infer types
        rows = list(reader)
        sample_rows = random.sample(rows, min(100, len(rows)))
        
        # Infer types for each column
        schema = {}
        for i, header in enumerate(headers):
            column_values = [row[i] for row in sample_rows if i < len(row)]
            schema[header] = infer_type(column_values)
        
        return {
            'columns': headers,
            'types': schema,
            'sample_size': len(sample_rows)
        }

def infer_type(values):
    """
    Simple type inference
    
    Parameters
    ----------
    values : list
        List of values to infer type for

    Returns
    -------
    str
        Inferred type
    """
    non_empty = [v for v in values if v.strip()]
    
    if not non_empty:
        return 'empty'
    
    # Try integer
    try:
        [int(v) for v in non_empty]
        return 'integer'
    except ValueError:
        pass
    
    # Try float
    try:
        [float(v) for v in non_empty]
        return 'float'
    except ValueError:
        pass

    # Try date
    try:
        [datetime.strptime(v, '%Y-%m-%d') for v in non_empty]
        return 'date'
    except ValueError:
        pass
    
    return 'string'

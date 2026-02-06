import zipfile
import logfire
from pathlib import Path

logfire.configure(send_to_logfire=True)

def zipfile_to_csv(zip_path: Path, csv_path: Path = Path("data/csv")) -> list[str]:
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
    csv_files = []
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

    except Exception as e:
        logfire.error(f"Error extracting zip file: {e}")
    return csv_files

from behave import fixture
from zipfile import ZipFile
from pathlib import Path
import os

def before_all(context):
    """
    Before running the tests, check if the fixtures path exists
    and if it doesn't, extract the fixtures zipfile to it.
    """
    fixture_destination_path = Path("tests/fixtures/data")
    zip_path = Path("tests/fixtures/data-fixtures.zip")

    if not fixture_destination_path.exists():
        os.mkdir(fixture_destination_path)

        with ZipFile(zip_path, 'r') as fixtures_zip:
            fixtures_zip.extractall(path = fixture_destination_path)
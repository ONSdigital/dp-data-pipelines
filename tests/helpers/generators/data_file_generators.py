import csv
from pathlib import Path
import sqlite3
from typing import Optional
import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom

import pandas as pd


def generate_xml(file_path: Path):
    root = ET.Element("root")

    # Add items
    items = ET.SubElement(root, "items")

    # Add item
    item1 = ET.SubElement(items, "item")
    item1.set("id", "1")
    name1 = ET.SubElement(item1, "name")
    name1.text = "item1"
    value1 = ET.SubElement(item1, "value")
    value1.text = "."

    rough_string = ET.tostring(root, "utf-8")
    reparsed = minidom.parseString(rough_string)
    pretty_xml = reparsed.toprettyxml(indent="  ")

    with open(file_path, "w") as xmlfile:
        xmlfile.write(pretty_xml)


def generate_excel(file_path: Path):
    data = {"id": [1, 2], "name": ["item1", "item2"], "value": [42.5, 13.7]}
    df = pd.DataFrame(data)

    df.to_excel(file_path, index=False)


def generate_csv(
    file_path: Path, rows: Optional[list[list[str]]] = [["id", "name", "value"]]
):
    with open(file_path, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        if rows:
            writer.writerows(rows)
            return


def generate_csdb(file_path: Path):
    conn = sqlite3.connect(str(file_path))
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE dummy_table(id)")
    conn.close()


def generate_data_file(file_path: Path):
    match file_path.suffix.strip("."):
        case "csv":
            return generate_csv(file_path)
        case "xls":
            return generate_excel(file_path)
        case "xlsx":
            return generate_excel(file_path)
        case "csdb":
            return generate_csdb(file_path)

    raise NotImplementedError(
        f"No data file generator for the file path {file_path} was found"
    )


def get_media_type_for_extension(extension: str):
    extension = extension.strip(".")

    match extension:
        case "csv":
            return "text/csv"
        case "csdb":
            return "text/plain"
        case "xls":
            return "application/vnd.ms-excel"
        case "sdmx":
            return "application/vnd.sdmx.structurespecificdata+xml"
        case "xlsx":
            return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        case "json":
            return "application/json"

    print(f"Could not find matching media type for extension {extension}")

    return "thisfiletype/wasnotfound"


def get_media_type_for_file_name(file_name: str):
    extension = file_name.split(".")[1]
    return get_media_type_for_extension(extension)

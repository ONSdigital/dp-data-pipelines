from pathlib import Path


def sdmx_sanity_check_v1(xml_file: Path):
    # check sdmx can be read in
    with open(xml_file, "r") as f:
        xml_content = f.read()
    assert xml_content.startswith("<?xml"), "file does not appear to be xml"

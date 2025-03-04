from pathlib import Path


def sdmx_sanity_check_v1(sdmx_file: Path):
    """
    Sanity check that the recieved sdmx file is actually an sdmx file.
    """
    # TODO use sdmx_path.suffix == ".xml" here
    assert sdmx_file.name.endswith(".xml"), "Invalid sdmx_file"

    try:
        with open(sdmx_file, "r") as f:
            f.read()
    except Exception as err:
        raise Exception(f"Failed to read in xml - {sdmx_file}") from err

from pathlib import Path


def sdmx_sanity_check_v1_file_extension(sdmx_file: Path):
    """
    Sanity check that the recieved sdmx file is actually an sdmx file.
    """
    assert sdmx_file.name.endswith(".xml"), "Invalid sdmx_file"


def sdmx_sanity_check_v1(sdmx_file: Path):
    """
    Sanity check that the recieved sdmx file is actually an sdmx file.
    """

    assert sdmx_file.name.endswith(".xml"), "Invalid sdmx_file"

    try:
        with open(sdmx_file, "r") as f:
            f.read()
        assert sdmx_file.name.endswith(".xml"), "Invalid sdmx_file"
    except Exception as err:
        raise Exception(f"Failed to read in xml - {sdmx_file}") from err

# TODO - these functions need creating properly
# these are just placeholder functions doing very basic things
# so that they can be picked up by other scripts
# delete both stub_ functions and change the other functions as required
# the output paths from smdx_default_v1() were just made up so change as required too


import json
from pathlib import Path

import pandas as pd


def stub_smdx_default_v1():
    """
    Placeholder function - A fake transform.
    Generates 2 very basic files
    """
    csv_file = Path("data.csv")
    json_file = Path("metadata.json")

    data = {"id": [1], "OBS_VALUE": [0]}
    df = pd.DataFrame(data)
    df.to_csv(csv_file, index=False)

    metadata_json = {
        "@context": "https://staging.idpd.uk/#ns",
        "title": "Title of a dataset",
    }
    with open(json_file, "w") as outfile:
        json.dump(metadata_json, outfile)


def stub_sdmx_sanity_check_v1():
    """
    Placeholder function - A fake sdmx sanity check that does nothing.
    """


def smdx_default_v1(sdmx_file: Path):
    stub_smdx_default_v1()
    return Path("data.csv"), Path("metadata.json")


def sdmx_sanity_check_v1(sdmx_file: Path):
    stub_sdmx_sanity_check_v1()

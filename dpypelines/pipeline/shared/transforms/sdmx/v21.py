from pathlib import Path

from dpypelines.pipeline.shared.transforms.sdmx.generate_versions_metadata import (
    generate_versions_metadata,
)
from dpypelines.pipeline.shared.transforms.sdmx.generic.v21.prototype.v1 import (
    xmlToCsvSDMX2_1,
)


def sdmx_generic_2_1_prototype_1(input_file: Path):

    csv_out = Path("data.csv")
    metadata_out = Path("metadata.json")

    xmlToCsvSDMX2_1(input_file, csv_out)
    generate_versions_metadata(csv_out, metadata_out)

    return csv_out, metadata_out

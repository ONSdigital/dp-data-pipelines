from pathlib import Path

from dpypelines.pipeline.shared.transforms.sdmx.compact.v20.prototype.v1 import (
    xmlToCsvSDMX2_0,
)
from dpypelines.pipeline.shared.transforms.sdmx.generate_versions_metadata import (
    generate_versions_metadata,
)


def sdmx_compact_2_0_prototype_1(input_file: Path):

    csv_out = Path("data.csv")
    metadata_out = Path("metadata.json")

    xmlToCsvSDMX2_0(input_file, csv_out)
    generate_versions_metadata(csv_out, metadata_out)

    return csv_out, metadata_out

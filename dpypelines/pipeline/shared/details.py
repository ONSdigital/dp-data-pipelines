from dpypelines.pipeline.shared.transforms.sdmx.v1 import (
    sdmx_sanity_check_v1,
    smdx_default_v1,
)

# Dev note: pointing at the default stub (does nothing) tranform for now
# will need updating to the prototype transform once that's in.
all_transform_details = {
    "sdmx.compact.v2.0.prototype.1": {
        "transform": smdx_default_v1,
        "transform_inputs": {"^data.xml$": sdmx_sanity_check_v1},
        "transform_kwargs": {},
    }
}

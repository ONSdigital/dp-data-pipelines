from dpypelines.pipeline.shared.transforms.sdmx.v1 import sdmx_sanity_check_v1, smdx_default_v1

all_transform_details = {
    "sdmx.default": {
        "transform": smdx_default_v1,
        "transform_inputs": {"*.sdmx": sdmx_sanity_check_v1},
        "transform_kwargs": {},
    }
}

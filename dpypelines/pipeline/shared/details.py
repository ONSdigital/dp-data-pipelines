from transforms.sdmx.v1 import smdx_default_v1, sdmx_sanity_check_v1

all_pipeline_details = {
	"sdmx.default": {
		"transform": smdx_default_v1,
		"transform_inputs": {
			"*.sdmx": sdmx_sanity_check_v1
		},
		"transform_kwargs": {}
    }
}

import json

import pandas as pd
import xmltodict

from dpypelines.pipeline.shared.transforms.utils import flatten_dict, pathify, set_key
from dpypelines.pipeline.shared.transforms.validate_transform_v21 import (
    check_columns_of_dataframes_are_unique,
    check_header_info,
    check_header_unpacked,
    check_length_of_dataframe_is_expected_length,
    check_obs_dicts_have_same_keys,
    check_read_in_sdmx,
    check_tidy_data_columns,
    check_xml_type,
    get_number_of_obs_from_xml_file,
)


def xmlToCsvSDMX2_1(input_path, output_path):

    check_read_in_sdmx(input_path)  # transform validation

    # Converting the XML file into a giant dictionary from the nested header dictionary
    with open(input_path, "r") as file:
        xml_content = file.read()
        data = xmltodict.parse(xml_content)
        check_xml_type(data)  # transform validation

    header = data["message:GenericData"]["message:Header"]
    for key in header.keys():
        header[(key.replace("message:", "").replace("common:", ""))] = header.pop(key)
    check_header_info(header)  # transform validatio

    expected_number_of_obs = get_number_of_obs_from_xml_file(
        input_path
    )  # transform validation

    tables = data["message:GenericData"]["message:DataSet"]["generic:Series"]

    series_dict = {}
    obs_dicts = []

    for table in tables:
        for element in table.items():
            if element[0] == "generic:SeriesKey":
                for sub_element in element[1]["generic:Value"]:
                    series_dict[sub_element["@id"]] = sub_element["@value"]
            elif element[0] == "generic:Attributes":
                for sub_element in element[1]["generic:Value"]:
                    series_dict[sub_element["@id"]] = sub_element["@value"]
            elif element[0] == "generic:Obs":
                for observation in element[1]:
                    observation_temp = {}
                    for sub_element in observation["generic:Attributes"][
                        "generic:Value"
                    ]:
                        observation_temp[sub_element["@id"]] = sub_element["@value"]
                    observation_temp.update(
                        {
                            "generic:ObsDimension": next(
                                iter(observation["generic:ObsDimension"].values())
                            )
                        }
                    )
                    observation_temp.update(
                        {
                            "generic:ObsValue": next(
                                iter(observation["generic:ObsValue"].values())
                            )
                        }
                    )
                    obs_dicts.append(series_dict | observation_temp)

    check_obs_dicts_have_same_keys(obs_dicts)  # transform validation
    obs_frame = pd.DataFrame(obs_dicts)
    check_length_of_dataframe_is_expected_length(
        obs_frame, expected_number_of_obs
    )  # transform validation

    header_dict = flatten_dict(header)
    check_header_unpacked(header_dict)  # transform validation
    header_df = pd.DataFrame([header_dict])

    header_repl_dict = header_df.to_dict(orient="list")
    for key in header_repl_dict:
        header_repl_dict[key] = header_repl_dict[key] * (len(obs_frame) - 1)

    header_repl_df = pd.DataFrame(header_repl_dict)
    header_frame = pd.concat([header_df, header_repl_df], ignore_index=True)
    check_length_of_dataframe_is_expected_length(
        header_frame, expected_number_of_obs
    )  # transform validation

    check_columns_of_dataframes_are_unique(
        obs_frame.columns, header_frame.columns
    )  # transform validation

    full_table = pd.concat([header_frame, obs_frame], axis=1)
    check_length_of_dataframe_is_expected_length(
        full_table, expected_number_of_obs
    )  # transform validation

    header_replace = {
        x: str(x).replace("@", "").replace("#", "") for x in full_table.columns
    }
    full_table.rename(columns=header_replace, inplace=True)
    full_table.rename(
        columns={
            "generic:ObsDimension": "TIME_PERIOD",
            "generic:ObsValue": "OBS_VALUE",
        },
        inplace=True,
    )
    check_tidy_data_columns(full_table.columns)  # transform validation

    full_table.to_csv(output_path, encoding="utf-8", index=False)
    return full_table

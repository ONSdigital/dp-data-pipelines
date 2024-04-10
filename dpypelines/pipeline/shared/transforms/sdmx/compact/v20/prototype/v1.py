import json
import xml.etree.ElementTree as ET

import pandas as pd
import xmltodict

from dpypelines.pipeline.shared.transforms.utils import convert, flatten_dict


def xmlToCsvSDMX2_0(input_path, output_path):

    # Converting the XML file into a giant dictionary from which we can extract the nested header dictionary
    with open(input_path, "r") as file:
        xml_content = file.read()
        data = xmltodict.parse(xml_content)

    header = data["CompactData"]["Header"]

    # The largest, top-level element (or section) in an XML document is called the root, which contains all other elements - child elements, subelements and so on.
    # Every part of the XML document tree (root included) has a tag that describes the element.

    # Attributes are name–value pair that exist within a start-tag or empty-element tag. An XML attribute can only have a single value and each attribute can appear at most once on each element.

    # The element tag is retrieved as .tab on Python ElementTree package.
    # The name-value pair attributes are retrieved as .attrib on Python ElementTree package.

    # Each block of observations tagged "Obs" within the same column headers is contained within a parent block tagged "Series". The respective tuples of dictionary are converted into 'flat' dictionaries and dropped into the series_dict and obs_dict.

    tree = ET.parse(input_path)
    root = tree.getroot()

    series_dict = {}
    obs_dict = {}
    for series in root.iter():
        if "Series" in series.tag:
            for obs in series.iter():
                if "Obs" in obs.tag:
                    convert(series.attrib, series_dict)
                    convert(obs.attrib, obs_dict)

    series_frame = pd.DataFrame(series_dict)
    obs_frame = pd.DataFrame(obs_dict)

    header_dict = flatten_dict(header)
    header_df = pd.DataFrame([header_dict])

    # Here the records on the header dataframe are replicated to match the length of the series and observation dataframes.
    repl_rows = header_df.loc[0].copy()
    header_frame = pd.concat(
        [header_df, pd.DataFrame([repl_rows] * (len(obs_frame) - 1))], ignore_index=True
    )

    full_table = pd.concat([header_frame, series_frame, obs_frame], axis=1)

    # the following is just tidying up the column headers so they are not filled with @ and such
    header_replace = {x: str(x).replace("@", "") for x in full_table.columns}
    full_table.rename(columns=header_replace, inplace=True)

    full_table.to_csv(output_path, encoding="utf-8", index=False)
    return full_table


def generate_versions_metadata(
    transformedCSV, outputPath, metadataTemplate=False, config=False, structureXML=False
):
    output = {}

    # this is just a placeholder before the metadata transform is approved
    with open(outputPath, "w") as f:
        json.dump({"to": "do"}, f)

    return output

import json

import pandas as pd
import xmltodict

from dpypelines.pipeline.shared.transforms.utils import pathify, set_key


def generate_versions_metadata(
    transformedCSV, outputPath, metadataTemplate=False, structureXML=False, config=False
):

    # Read in Structure XML provided with the SDMX and tidyCSV we created earlier in the transform
    if structureXML:
        with open(structureXML, "r") as file:
            xml_content = file.read()
            data = xmltodict.parse(xml_content)

    tidyCSV = pd.read_csv(transformedCSV)

    # Pull out the dataset title which we'll use later
    dataset_title = tidyCSV["TITLE"].iloc[0]

    # Get a list of the concepts and key families from the structure XML which contain info on the data dimensions, at the moment we're basically just using this for the datatype and dimension name where possible
    # this then gets thrown into a dictionary with an entry for each possible dimension header
    if structureXML:
        dimensions = {}

        metadata = data["mes:Structure"]["mes:Concepts"]["str:ConceptScheme"][
            "str:Concept"
        ]

        for concept in metadata:
            set_key(dimensions, concept["@id"], concept["@id"].lower())
            set_key(dimensions, concept["@id"], concept["str:Name"]["#text"])

        metadata = data["mes:Structure"]["mes:KeyFamilies"]["str:KeyFamily"][
            "str:Components"
        ]["str:Dimension"]

        for concept in metadata:
            set_key(
                dimensions,
                concept["@conceptRef"],
                concept["str:TextFormat"]["@textType"],
            )

    # Here we're going through each header in the tidyCSV and attempting to match it up with the DSD informaion we got before, for other headers (such as the metadata columns) we assume the datatype is just a string
    full_dimensions = {}

    if structureXML:
        for column in tidyCSV.columns:
            if column in dimensions.keys():
                full_dimensions[column] = column
                if len(dimensions[column]) == 3:
                    set_key(full_dimensions, column, dimensions[column][0])
                    set_key(full_dimensions, column, dimensions[column][1])
                    set_key(full_dimensions, column, dimensions[column][2])
                else:
                    set_key(full_dimensions, column, dimensions[column][0])
                    set_key(full_dimensions, column, dimensions[column][1])
                    set_key(full_dimensions, column, "string")
            else:
                full_dimensions[column] = column
                set_key(full_dimensions, column, column.lower())
                set_key(full_dimensions, column, column.lower())
                set_key(full_dimensions, column, "string")
    else:
        for column in tidyCSV.columns:
            full_dimensions[column] = column
            set_key(full_dimensions, column, column.lower())
            set_key(full_dimensions, column, column.lower())
            set_key(full_dimensions, column, "string")

    full_dimensions_list = list(full_dimensions.values())

    # Read in our metadata template

    if metadataTemplate:
        with open(metadataTemplate) as json_data:
            versions_template = json.load(json_data)
    else:
        versions_template = {}

    # now a very terrible and hardcoded implementation of applying what little metadata we have to as many fields as possible

    versions_template["title"] = dataset_title
    versions_template["description"] = ""
    versions_template["summary"] = ""
    versions_template["identifier"] = (
        "https://staging.idpd.uk/datasets/" + pathify(dataset_title) + "/editions"
    )
    if structureXML:
        versions_template["issued"] = (
            data["mes:Structure"]["mes:Header"]["mes:Prepared"].split(".")[0] + "Z"
        )
    else:
        versions_template["issued"] = ""
    versions_template["license"] = (
        "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/"
    )
    versions_template["modified"] = (
        tidyCSV["Extracted"].iloc[0].split("+")[0] + "Z"
    )  # This is working off the assumption that every extraction date is a new modification of the data
    versions_template["next_release"] = (
        ""  # could we infer this from issued date and release frequency if we include that in the config?
    )
    versions_template["publisher"] = {
        "email": "",
        "name": "",
        "telephone": "",
    }  # config file?
    versions_template["frequency"] = ""  # config file?
    versions_template["spatial_coverage"] = list(tidyCSV.REF_AREA.unique())[
        0
    ]  # this will need investigation into whether this is accurate to what we need for this field
    versions_template["spatial_resolution"] = list(
        tidyCSV.COUNTERPART_AREA.unique()
    )  # this will need investigation into whether this is accurate to what we need for this field
    versions_template["temporal_coverage"] = (
        "start: "
        + str(min(tidyCSV.TIME_PERIOD))
        + ", end: "
        + str(max(tidyCSV.TIME_PERIOD))
    )  # I'll need to input on the formatting of this field cause the previous iteration was a dictionry but the spec has it now as a string
    versions_template["temporal_resolution"] = list(tidyCSV.TIME_FORMAT.unique())[
        0
    ]  # this will need investigation into whether this is accurate to what we need for this field
    versions_template["contact_point"] = {
        "email": "",
        "name": "",
        "telephone": "",
    }  # config file?
    versions_template["keywords"] = ["", ""]
    versions_template["themes"] = ["", ""]

    columns = []
    for i in full_dimensions_list:
        column = {}
        column["component_type"] = ""
        column["datatype"] = i[3]
        column["name"] = i[0]
        column["titles"] = i[1]
        column["property_url"] = ""
        column["value_url"] = ""
        column["codelist_url"] = ""
        column["sub_property_of"] = ""
        columns.append(column)

    table_schema = {}
    table_schema["about_url"] = ""
    table_schema["column"] = columns

    distributions = []

    distribution = {
        "checksum": "",
        "@id": "",
        "byte_size": "",
        "media_type": "",
        "download_url": "",
        "described_by": "",
        "table_schema": table_schema,
    }

    distributions.append(
        distribution
    )  # at some point we'll need to pull in any previous existing distributions but im assuming a lot of this script will change when we get that far

    versions_template["distributions"] = distributions
    versions_template["version_notes"] = [""]
    versions_template["@context"] = ""
    versions_template["@id"] = ""  # Is this the same as the initial identifier?
    versions_template["@type"] = "dcat:dataset"
    versions_template["etag"] = ""  # ?
    versions_template["is_based_on"] = {"id": "", "type": ""}  # ?
    versions_template["_embedded"] = {
        "code_list": "string",
        "identifier": "string",
        "label": "string",
        "name": "string",
    }
    versions_template["type"] = ""  # ?
    versions_template["state"] = ""  # ?

    # dump out metadata file

    # with open(str(outputPath) + pathify(dataset_title) + "_" + str(datetime.now().strftime("%Y-%m")) + "-metadata.json", "w") as outfile:
    #    json.dump(versions_template, outfile, ensure_ascii=False, indent=4)
    with open(outputPath, "w") as outfile:
        json.dump(versions_template, outfile, ensure_ascii=False, indent=4)

    return versions_template

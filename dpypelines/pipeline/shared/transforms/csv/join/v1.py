import pandas as pd
import xmltodict
import csv
import os

from dpypelines.pipeline.shared.transforms.utils import flatten_dict
from dpypelines.pipeline.shared.transforms.validate_csv import (
    validate_csv
)
from dpypelines.pipeline.shared.transforms.validate_transform_csvJoin import (
    check_multiple_files,
    check_headers_match,
    check_length_of_csv_is_as_expected,
    check_tidy_data_columns,
)


def joinCSV(input_path, output_path):

    files = [x for x in os.listdir(input_path) if '.csv' in str(x)] # get csvs in input directory 
        
    print(files)

    check_multiple_files(files) # this transform should only be run to join multiple csv files, so if there's only 1 provided then something is wrong


    # Get the headers for each of the provded csvs 
    headers = []
    dataframes = []
    for i in files:
        #print("Invalid CSV File: " + str(i))
        with open(str(files[0]), 'r') as f:
            dict_reader = csv.DictReader(f)
            #get header fieldnames from DictReader and store in list
            header = dict_reader.fieldnames
            headers.append(header)
        validate_csv(i, header)
        dataframes.append(pd.read_csv(i))

    check_headers_match(headers) # check the headers for each of the input files match

    full_table = pd.concat(dataframes)

    check_length_of_csv_is_as_expected(
        dataframes, full_table
    )

    header_replace = {
        x: str(x).replace("@", "").replace("#", "") for x in full_table.columns
    }
    full_table.rename(columns=header_replace, inplace=True)
   
    check_tidy_data_columns(full_table.columns)  # transform validation

    full_table.to_csv(output_path, encoding="utf-8", index=False)
    return full_table
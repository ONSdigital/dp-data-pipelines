import json
import xmltodict
import pandas as pd
from datetime import datetime, date
from dpypelines.pipeline.shared.transforms.utils import set_key, pathify
        
def xmlToCsvSDMX2_0(input_path, output_path):

    # Here we're turning the XML file into a giant dictionary we can pull apart
    with open(input_path, 'r') as file:
        xml_content = file.read()
        data = xmltodict.parse(xml_content)

    # I hate everything about how I've got the header info here but it works with multiple types of SDMX
    
    header = data["CompactData"]['Header']

    header_dict = {}

    # The following loops go through the header and pull out as much info as possible, unfortunately due to the inconsistent depth tags there's a bunch of nested loops
    # could optimise but only adding a nested loop when it see's a tag that you would expect to have another nested tag but then it would break if any tag breaks this expectation

    for header, value in header.items():
        if '{' not in str(value):
            header_dict[header.replace('message:', '')] = value
        else:
            for i, j in value.items():
                if '{' not in str(j):
                    header_dict[str(header +  ' ' + i).replace('message:', '').replace('common', '')] = j
                else:
                    for k, l in j.items():
                        if '{' not in str(l):
                            header_dict[str(header +  ' ' + i +  ' ' + k).replace('message:', '').replace('common:', '')] = l
                        else:
                            for m, n in l.items():
                                header_dict[str(header +  ' ' + i +  ' ' + k +  ' ' + m).replace('message:', '').replace('common:', '')] = n

    header_frame = pd.DataFrame([header_dict])

    # each set of observations that has the same column variables is contained to blocks with series' tags inside a big Dataset tag so we can grab all the data here to then break it down

    tables = data["CompactData"]['na_:DataSet']['na_:Series']
    table_header = data["CompactData"]['na_:DataSet']

    # here we're building a list of the headers for the dataset using the info inside the series tag. All of the data is in the obs tag so we can ignore that one

    headers = []

    for item in table_header.values():
        for i in item[0].items():
            if i[0]!='na_:Obs':
                headers.append(i[0])

    output = []

    # and this is the main loop which goes through each series block and pulls out the observational data and sticks in under the right header where it exists, then adding the new columns for the remaining variables inside the obs tags

    for table_index in range(len(tables)):
        list = tables[table_index] # get a series block
        headers_df = header_frame # create an df of the headers we have so far
        obs_df = pd.DataFrame() # create an empty df for the headers remaining in the obs tag

        for item in list.items(): # as we move through the series block we go through each value and see if it matches one of the headers we already have
            if item[0] in headers: # if the header matches we insert the value for this header
                temp = []
                temp.append(item[1])
                headers_df[item[0]] = temp
                # I think this is a point of performance loss, we should only have to check the headers once per series block rather than looping past it every time (TODO)
            else: # if not then its an observation so we crete a temporary mini df with the 4 values in the obs tag and then append it to a dataframe containing all the observations for this series block, then repeat for each obs tag
                for entry in item[1]:
                    temp_df = pd.DataFrame()
                    for obs in entry.items():
                        temp = []
                        temp.append(obs[1])
                        temp_df[obs[0]] = temp
                    obs_df = pd.concat([obs_df,temp_df])
            
        table_df = pd.concat([headers_df, obs_df], axis = 1) # merge the headers to each observation

        output.append(table_df) # add to a list of all the series blocks that have been processed 

    full_table = pd.concat(output) # merge all the series blocks together

    # the following is just tidying up the column headers so theyre not filled with @ and such

    header_replace = { x: str(x).replace('@', '') for x in full_table.columns }
    full_table.rename(columns=header_replace, inplace=True)

    full_table.to_csv(output_path, encoding='utf-8', index=False)
    return full_table

def generate_versions_metadata(transformedCSV, outputPath, metadataTemplate = False, config = False, structureXML = False):
        output = {}

        #this is just a placeholder before the metadata transform is approved
        with open(outputPath, "w") as f:
            json.dump({"to": "do"}, f)

        return output

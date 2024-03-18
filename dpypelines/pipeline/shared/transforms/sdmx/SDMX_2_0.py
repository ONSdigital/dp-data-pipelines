import json
import xmltodict
import pandas as pd
from datetime import datetime, date
from dpypelines.pipeline.functions.utils import pathify

def set_key(dictionary, key, value):
     if key not in dictionary:
         dictionary[key] = value
     elif type(dictionary[key]) == list:
         dictionary[key].append(value)
     else:
         dictionary[key] = [dictionary[key], value]
         

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

    # the following is just tidying up the column headers so theyre not filled with @ and such, pretty sure there's an easier way to do this but hey ho

    header_replace = { x: str(x).replace('@', '') for x in full_table.columns }
    full_table.rename(columns=header_replace, inplace=True)

    full_table.to_csv(output_path, encoding='utf-8', index=False)


def generate_editions_metadata(transformedCSV, structureXML, outputPath, metadataTemplate, config = False):

    # Read in Structure XML and tidyCSV 
    with open(structureXML, 'r') as file:
            xml_content = file.read()
            data = xmltodict.parse(xml_content)

    tidyCSV = pd.read_csv(transformedCSV)

    # Pull out the dataset title which we'll use later
    dataset_title = tidyCSV['TITLE'].iloc[0]

    # Get a list of the concepts and key families from the structure XML which contain info on the data dimensions, at the moment we're basically just using this for the datatype and dimension name where possible
    # this then gets thrown into a dictionary with an entry for each possible dimension header
    dimensions = {}

    metadata = data["mes:Structure"]['mes:Concepts']['str:ConceptScheme']['str:Concept']

    for concept in metadata:
        set_key(dimensions, concept['@id'],  concept['@id'].lower())
        set_key(dimensions, concept['@id'],  concept['str:Name']['#text'])
        
    metadata = data["mes:Structure"]['mes:KeyFamilies']['str:KeyFamily']['str:Components']['str:Dimension']

    for concept in metadata:
        set_key(dimensions, concept['@conceptRef'], concept['str:TextFormat']['@textType'])

    # Here we're going through each header in the tidyCSV and attempting to match it up with the DSD informaion we got before, for other headers (such as the metadata columns) we assume the datatype is just a string
    full_dimensions = {}

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
                set_key(full_dimensions, column, 'string')
        else:
            full_dimensions[column] = column
            set_key(full_dimensions, column, column.lower())
            set_key(full_dimensions, column, column.lower())
            set_key(full_dimensions, column, 'string')

    full_dimensions_list = list(full_dimensions.values())

    # Read in our metadata template

    with open(metadataTemplate) as json_data:
            editions_template = json.load(json_data)

    # now a very terrible and hardcoded implementation of applying what little metadata we have to as many fields as possible

    editions_template['@id'] = 'https://staging.idpd.uk/datasets/' + pathify(dataset_title) + '/editions'
    editions_template['title'] = dataset_title

    current_edition = editions_template['editions'][0] # This will get the first entry in the editions list to use as a template (TODO: include editions list length to check if this will be the first edition or an addition and create addendum)
        
    current_edition['@id'] = 'https://staging.idpd.uk/datasets/' + pathify(dataset_title) + '/editions/' + str(datetime.now().strftime("%Y-%m"))
    current_edition['in_series'] = 'https://staging.idpd.uk/datasets/' + pathify(dataset_title)
    current_edition['identifier'] = str(datetime.now().strftime("%Y-%m"))
    current_edition['title'] = dataset_title
    current_edition['summary'] = "" # Doesnt appear to have any summary or description in the supporting XML which isnt surprising but means we have nothing for these 2 fields at entry
    current_edition['description'] = ""
    current_edition['publisher'] = "" # not sure whether the sender/reciever covers publisher/creator so will leave this blank for now
    current_edition['creator'] = ""
    current_edition['contact_point'] = {'name': "", 'email' : ""} # Take from config file
    current_edition['topics'] = "" # could we infer this from the structure file?
    current_edition['frequency'] = "" # is this something we should include in the config file?
    current_edition['keywords'] = ["", ""] # anyway some of this could be infered?
    current_edition['issued'] = data["mes:Structure"]['mes:Header']['mes:Prepared'].split('.')[0] + 'Z' # Not sure whether there is a better way to get issued date as it seems to just take the date it was extracted
    current_edition['modified'] = tidyCSV['Extracted'].iloc[0].split('+')[0] + 'Z' # This is working off the assumption that every extraction date is a new modification of the data
    current_edition['spatial_resolution'] = list(tidyCSV.COUNTERPART_AREA.unique()) # this is certainly not gonna be correct in the long run but we can replace it later or remove it 
    current_edition['spatial_coverage'] = list(tidyCSV.REF_AREA.unique()) # this is certainly not gonna be correct in the long run but we can replace it later or remove it 
    current_edition['temporal_resolution'] = list(tidyCSV.TIME_FORMAT.unique())
    current_edition['temporal_coverage'] = {'start' : min(tidyCSV.TIME_PERIOD), 'end' : max(tidyCSV.TIME_PERIOD)} # This will need a lot of formatting/conditions to end up as datetime from what could be varying format of input
    current_edition['versions_url'] = 'https://staging.idpd.uk/datasets/' + pathify(dataset_title) + '/editions/' + str(datetime.now().strftime("%Y-%m")) + '/versions'
    current_edition['versions'] = {'@id': 'https://staging.idpd.uk/datasets/' + pathify(dataset_title) + '/editions/' + str(datetime.now().strftime("%Y-%m")) + '/versions/1',
                                'issued': data["mes:Structure"]['mes:Header']['mes:Prepared'].split('.')[0] + 'Z'}
    current_edition['next_release'] = "" # could we infer this from issued date and frequency if we include that in the config?

    columns = []
    for i in full_dimensions_list:
        column = {}
        column['name'] = i[0]
        column['datatype'] = i[3]
        column['titles'] = i[1]
        column['description'] = i[2]
        columns.append(column)
    current_edition['table_schema'] = {'columns' : columns}
    editions_template['editions'][0] = current_edition # just a reminder this is currently for a first edition of a dataset so it will put itself as the only entry

    editions_template['count'] = len(editions_template['editions'])
    editions_template['offset'] = 0 # not sure what this is tbh

    # dump out metadata file

    with open(outputPath + pathify(dataset_title) + "_" + str(datetime.now().strftime("%Y-%m")) + "-metadata.json", "w") as outfile:
        json.dump(editions_template, outfile, ensure_ascii=False, indent=4)



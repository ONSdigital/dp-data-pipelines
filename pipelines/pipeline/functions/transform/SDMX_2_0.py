import json
import xmltodict
import pandas as pd
from gssutils import *
from datetime import datetime, date


def set_key(dictionary, key, value):
     if key not in dictionary:
         dictionary[key] = value
     elif type(dictionary[key]) == list:
         dictionary[key].append(value)
     else:
         dictionary[key] = [dictionary[key], value]
         

def xmlToCsvSDMX2_0(input_path, output_path):

    with open(input_path, 'r') as file:
        xml_content = file.read()
        data = xmltodict.parse(xml_content)

    # I hate everything about how I've got the header info here but it works with multiple types of SDMX
        
    header = data["CompactData"]['Header']

    header_dict = {}

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

    tables = data["CompactData"]['na_:DataSet']['na_:Series']
    table_header = data["CompactData"]['na_:DataSet']

    headers = []

    for item in table_header.values():
        for i in item[0].items():
            if i[0]!='na_:Obs':
                headers.append(i[0])

    output = []

    for i in range(len(tables)):
        list = tables[i]
        table = list['na_:Obs']
        headers_df = header_frame
        obs_df = pd.DataFrame()

        for i in list.items():
            if i[0] in headers:
                temp = []
                temp.append(i[1])
                headers_df[i[0]] = temp
            else:
                for entry in i[1]:
                    temp_df = pd.DataFrame()
                    for obs in entry.items():
                        temp = []
                        temp.append(obs[1])
                        temp_df[obs[0]] = temp
                    obs_df = pd.concat([obs_df,temp_df])
            
        table_df = pd.concat([headers_df, obs_df], axis = 1)

        output.append(table_df)

    full_table = pd.concat(output)

    headerReplace = [s.replace('@', '') for s in full_table.columns]
    headerNorm = {}
    for key in full_table.columns:
        for value in headerReplace:
            headerNorm[key] = value
            headerReplace.remove(value)
            break
    full_table.rename(columns=headerNorm, inplace=True)

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

    for i in tidyCSV.columns:
        if i in dimensions.keys():
            full_dimensions[i] = i
            if len(dimensions[i]) == 3:
                set_key(full_dimensions, i, dimensions[i][0])
                set_key(full_dimensions, i, dimensions[i][1])   
                set_key(full_dimensions, i, dimensions[i][2])          
            else:
                set_key(full_dimensions, i, dimensions[i][0])
                set_key(full_dimensions, i, dimensions[i][1])
                set_key(full_dimensions, i, 'string')
        else:
            full_dimensions[i] = i
            set_key(full_dimensions, i, i.lower())
            set_key(full_dimensions, i, i.lower())
            set_key(full_dimensions, i, 'string')

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



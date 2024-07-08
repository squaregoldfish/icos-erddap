"""
Set up data files and datasets.xml for an ERDDAP server
"""
import toml
import logging
import traceback
import database
import os
import re
import icos
from icoscp_core.icos import data
from Attribute import Attribute

_LOG_FILE_ = "populate_erddap.log"


def main(config):
    try:
        logging.basicConfig(filename=_LOG_FILE_,
                            format='%(asctime)s %(levelname)s - %(message)s', level=config["log_level"])
        _check_config(config)

        logging.info("Checking for required ERDDAP updates")

        with database.connect() as db:
            datasets = database.get_pids_with_status(db)

            for dataset in datasets:
                dataset_ok = True

                # Download the new dataset if required
                if dataset["new"]:
                    logging.info(f"Downloading dataset {dataset["pid"]}")
                    dataset_dir = os.path.join(config["datasets_dir"], dataset["pid"])
                    os.makedirs(dataset_dir, exist_ok=True)

                    try:
                        data.save_to_folder(icos.make_data_object_uri(dataset["pid"]), dataset_dir)
                        database.clear_new(db, dataset["pid"])
                    except Exception:
                        logging.error(f"Unable to download dataset {dataset["pid"]}")
                        logging.error(traceback.format_exc())
                        dataset_ok = False

                if dataset_ok:
                    if dataset["updated"]:
                        try:
                            _make_datasets_xml_entry(db, dataset["pid"])
                        except Exception:
                            logging.error(f"Failed to build datasets.xml for dataset {dataset["pid"]}")
                            logging.error(traceback.format_exc())
                            dataset_ok = False

            _write_datasets_xml(config, db)
    except Exception as e:
        logging.critical(f"Unhandled exception: {e}")
        logging.critical(traceback.format_exc())


def _make_datasets_xml_entry(db, pid):
    metadata = database.get_metadata(db, pid)

    entry = '<dataset type="EDDTableFromAsciiFiles" '
    entry += f'datasetID="{pid}" '
    entry += f'active="true"'
    entry += '>\n'

    spec = metadata['data_object']['spec'].split('/')[-1]
    if spec == 'icosOtcL2Product':
        entry += _make_soop_entry(pid, metadata, config)
    elif spec == 'icosOtcFosL2Product':
        entry += _make_fos_entry(pid, metadata, config)
    else:
        raise ValueError(f'Unrecognised dobj spec {spec}')

    entry += '</dataset>\n'

    database.write_datasets_xml(db, pid, entry)


def _make_soop_entry(pid, metadata, config):
    # Base setup
    entry = '<reloadEveryNMinutes>10080</reloadEveryNMinutes>\n'
    entry += '<updateEveryNMillis>10000</updateEveryNMillis>\n'
    entry += f'<fileDir>{os.path.join(config['datasets_dir'], pid)}</fileDir>\n'
    entry += '<fileNameRegex>.*\\.csv</fileNameRegex>\n'
    entry += '<recursive>false</recursive>\n'
    entry += '<pathRegex>.*</pathRegex>\n'
    entry += '<metadataFrom>last</metadataFrom>\n'
    entry += '<standardizeWhat>0</standardizeWhat>\n'
    entry += '<charset>UTF-8</charset>\n'
    entry += '<columnSeparator>,</columnSeparator>\n'
    entry += '<columnNamesRow>1</columnNamesRow>\n'
    entry += '<firstDataRow>2</firstDataRow>\n'
    entry += '<sortedColumnSourceName>Date/Time</sortedColumnSourceName>\n'
    entry += '<sortFilesBySourceNames>Date/Time</sortFilesBySourceNames>\n'
    entry += '<fileTableInMemory>false</fileTableInMemory>\n'

    # Attributes
    entry += '<addAttributes>\n'
    entry += _make_attribute_xml(Attribute('cdm_data_type', 'Point'))
    entry += _make_attribute_xml(Attribute('Conventions', 'COARDS, CF-1.6, ACDD-1.3'))
    entry += _make_attribute_xml(Attribute('infoUrl', icos.make_data_object_uri(pid)))
    entry += _make_attribute_xml(Attribute('institution', 'ICOS RI; TK'))
    entry += _make_attribute_xml(Attribute('keywords', 'TK (from ICOS?)'))
    entry += _make_attribute_xml(Attribute('license',
                                           'CC BY 4.0/ICOS Data Licence&lt;br/&gt;https://www.icos-cp.eu/data-services/about-data-portal/data-license'))
    entry += _make_attribute_xml(Attribute('sourceUrl', icos.make_data_object_uri(pid)))
    entry += _make_attribute_xml(Attribute('standard_name_vocabulary', 'CF Standard Name Table v70'))
    entry += _make_attribute_xml(Attribute('subsetVariables', '<!--TK-->'))
    entry += _make_attribute_xml(Attribute('summary', 'TK'))
    entry += _make_attribute_xml(Attribute('title', f'TK - {metadata['data_object']['fileName']}'))

    entry += '</addAttributes>\n'

    # Data Variables
    entry += _generate_data_variables_xml('soop_variables.csv')

    # Manually created fixed values
    # _trajectory_id (will be EXPO CODE)

    return entry


def _make_fos_entry(pid, metadata, config):
    return "<!--FOS-->\n"


def _generate_data_variables_xml(var_file):

    xml = ''

    with open(f'erddap/{var_file}') as var_in:
        # Skip the first line - it's a header
        var_in.readline()

        fields = var_in.readline().split(',')
        while len(fields) > 1:
            attributes = list()
            if len(fields[7]) > 0:
                attributes.append(Attribute('colorBarMinimum', fields[7], 'float'))
            if len(fields[8]) > 0:
                attributes.append(Attribute('colorBarMaximum', fields[8], 'float'))
            if len(fields[9]) > 0:
                attributes.append(Attribute('_FillValue', fields[9]))

            if len(fields[10].strip()) > 0:
                other_attrs = fields[10].strip().split(';')
                for attr in other_attrs:
                    m = re.search('(.*):(.*)=(.*)', attr)
                    if not m:
                        raise ValueError(f'Invalid attribute specification "{attr}"')
                    attr_name, attr_type, attr_value = m.group(1, 2, 3)
                    if len(attr_type) == 0:
                        attributes.append(Attribute(attr_name, attr_value))
                    else:
                        attributes.append(Attribute(attr_name, attr_value, attr_type))

            xml += _make_data_variable_xml(fields[0], fields[1], fields[2], fields[3], fields[4], fields[5],
                                           fields[6], attributes)

            fields = var_in.readline().split(',')


    return xml
def _make_data_variable_xml(source_name, destination_name, data_type, units, ioos_category, long_name,
                            standard_name, other_attributes):
    xml = '<dataVariable>\n'
    xml += f'<sourceName>{source_name}</sourceName>\n'
    xml += f'<destinationName>{destination_name}</destinationName>\n'
    xml += f'<dataType>{data_type}</dataType>\n'

    xml += '<addAttributes>\n'
    xml += _make_attribute_xml(Attribute('units', units))
    xml += _make_attribute_xml(Attribute('ioos_category', ioos_category))
    xml += _make_attribute_xml(Attribute('source_name', source_name))
    xml += _make_attribute_xml(Attribute('long_name', long_name))
    if standard_name is not None:
        xml += _make_attribute_xml(Attribute('standard_name', standard_name))

    for attribute in other_attributes:
        xml += _make_attribute_xml(attribute)

    xml += '</addAttributes>\n'

    xml += '</dataVariable>\n'

    return xml


def _make_attribute_xml(attribute):
    attr = f'<att name="{attribute.name}"'
    if attribute.attr_type is not None:
        attr += f' type="{attribute.attr_type}"'
    attr += f'>{attribute.value}</att>\n'

    return attr


def _write_datasets_xml(config, db):
    with open(config['datasets_xml_location'], 'w') as out:
        # Write the erddap preamble and open root node
        out.write('<?xml version="1.0" encoding="ISO-8859-1" ?>\n')
        out.write('<erddapDatasets>\n')

        # Copy the contents of the start template into the file
        with open('erddap/datasets_xml_start.xml', 'r') as start:
            out.write(start.read())

        # Add the erddap for the datasets
        out.write(database.get_datasets_xml(db))

        # Copy the contents of the end template into the file
        with open('erddap/datasets_xml_end.xml', 'r') as end:
            out.write(end.read())

        # Close the root node
        out.write('</erddapDatasets>\n')


def _check_config(config):
    try:
        os.makedirs(config["datasets_dir"], exist_ok=True)
    except Exception:
        logging.critical("Cannot create datasets directory")
        logging.critical(traceback.format_exc())
        exit(1)


if __name__ == "__main__":
    with open("config.toml") as f:
        config = toml.load(f)

    main(config)

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

# VARIABLE COLUMN IDs
SOURCE_NAME_COL = 0
DESTINATION_COL = 1
DATA_TYPE_COL = 2
UNITS_COL = 3
IOOS_CATEGORY_COL = 4
LONG_NAME_COL = 5
STANDARD_NAME_COL = 6
COLOUR_BAR_MIN_COL = 7
COLOUR_BAR_MAX_COL = 8
FILL_VALUE_COL = 9
OTHER_ATTRIBUTES_COL = 10


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
    # Common base details
    entry = _make_common_base(pid, config)

    # Attributes
    entry += '<addAttributes>\n'
    entry += _make_common_attributes(pid, metadata)

    entry += '</addAttributes>\n'

    # Data Variables
    entry += _generate_data_variables_xml('soop_variables.csv')

    entry += _variable_from_global_attribute('expocode', 'EXPOCODE', 'EXPOCODE')
    entry += _variable_from_global_attribute('platform_id', 'platform_id', 'ICES Platform Code')

    # Manually created fixed values
    # _trajectory_id (will be EXPO CODE)

    return entry


def _make_fos_entry(pid, metadata, config):
    # Common base details
    entry = _make_common_base(pid, config)

    # Attributes
    entry += '<addAttributes>\n'
    entry += _make_common_attributes(pid, metadata)

    entry += '</addAttributes>\n'

    # Data Variables
    entry += _generate_data_variables_xml('fos_variables.csv')

    # Manually created fixed values
    # _trajectory_id (will be EXPO CODE)

    return entry


def _make_common_base(pid, config):
    base = '<reloadEveryNMinutes>10080</reloadEveryNMinutes>\n'
    base += '<updateEveryNMillis>10000</updateEveryNMillis>\n'
    base += f'<fileDir>{os.path.join(config['datasets_dir'], pid)}</fileDir>\n'
    base += '<fileNameRegex>.*\\.csv</fileNameRegex>\n'
    base += '<recursive>false</recursive>\n'
    base += '<pathRegex>.*</pathRegex>\n'
    base += '<metadataFrom>last</metadataFrom>\n'
    base += '<standardizeWhat>0</standardizeWhat>\n'
    base += '<charset>UTF-8</charset>\n'
    base += '<columnSeparator>,</columnSeparator>\n'
    base += '<columnNamesRow>1</columnNamesRow>\n'
    base += '<firstDataRow>2</firstDataRow>\n'
    base += '<sortedColumnSourceName>Date/Time</sortedColumnSourceName>\n'
    base += '<sortFilesBySourceNames>Date/Time</sortFilesBySourceNames>\n'
    base += '<fileTableInMemory>false</fileTableInMemory>\n'
    return base


def _make_common_attributes(pid, metadata):
    common_attributes = _make_attribute_xml(Attribute('cdm_data_type', 'Point'))
    common_attributes += _make_attribute_xml(Attribute('Conventions', 'COARDS, CF-1.6, ACDD-1.3'))
    common_attributes += _make_attribute_xml(Attribute('infoUrl', icos.make_data_object_uri(pid)))
    common_attributes += _make_attribute_xml(Attribute('institution', 'ICOS RI; TK'))
    common_attributes += _make_attribute_xml(Attribute('keywords', 'TK (from ICOS?)'))
    common_attributes += _make_attribute_xml(Attribute('license',
                                                       'CC BY 4.0/ICOS Data Licence: https://www.icos-cp.eu/data-services/about-data-portal/data-license'))
    common_attributes += _make_attribute_xml(Attribute('sourceUrl', icos.make_data_object_uri(pid)))
    common_attributes += _make_attribute_xml(Attribute('standard_name_vocabulary', 'CF Standard Name Table v70'))
    common_attributes += _make_attribute_xml(Attribute('summary', 'TK'))
    common_attributes += _make_attribute_xml(Attribute('title', f'TK - {metadata['data_object']['fileName']}'))
    common_attributes += _make_attribute_xml(
        Attribute('expocode', f'{metadata['data_object']['fileName'].split(".")[0]}'))
    common_attributes += _make_attribute_xml(Attribute('platform_id', f'{metadata['data_object']['fileName'][:4]}'))

    return common_attributes


def _generate_data_variables_xml(var_file):
    xml = ''

    with open(f'erddap/{var_file}') as var_in:
        # Skip the first line - it's a header
        var_in.readline()

        fields = var_in.readline().split(',')
        while len(fields) > 1:
            attributes = list()
            if len(fields[COLOUR_BAR_MIN_COL]) > 0:
                attributes.append(Attribute('colorBarMinimum', fields[COLOUR_BAR_MIN_COL], 'float'))
            if len(fields[COLOUR_BAR_MAX_COL]) > 0:
                attributes.append(Attribute('colorBarMaximum', fields[COLOUR_BAR_MAX_COL], 'float'))
            if len(fields[FILL_VALUE_COL]) > 0:
                attributes.append(Attribute('_FillValue', fields[FILL_VALUE_COL]))

            if len(fields[OTHER_ATTRIBUTES_COL].strip()) > 0:
                other_attrs = fields[OTHER_ATTRIBUTES_COL].strip().split(';')
                for attr in other_attrs:
                    m = re.search('(.*):(.*)=(.*)', attr)
                    if not m:
                        raise ValueError(f'Invalid attribute specification "{attr}"')
                    attr_name, attr_type, attr_value = m.group(1, 2, 3)
                    if len(attr_type) == 0:
                        attributes.append(Attribute(attr_name, attr_value))
                    else:
                        attributes.append(Attribute(attr_name, attr_value, attr_type))

            xml += _make_data_variable_xml(fields[SOURCE_NAME_COL], fields[DESTINATION_COL], fields[DATA_TYPE_COL],
                                           fields[UNITS_COL], fields[IOOS_CATEGORY_COL], fields[LONG_NAME_COL],
                                           fields[STANDARD_NAME_COL], attributes)

            fields = var_in.readline().split(',')

    return xml


def _variable_from_global_attribute(attribute, destination, long_name):
    xml = '<dataVariable>\n'
    xml += f'<sourceName>global:{attribute}</sourceName>\n'
    xml += f'<destinationName>{destination}</destinationName>\n'
    xml += '<dataType>String</dataType>\n'

    xml += '<addAttributes>\n'
    xml += _make_attribute_xml(Attribute('ioos_category', 'Unknown'))
    xml += _make_attribute_xml(Attribute('long_name', long_name))
    xml += '</addAttributes>\n'

    xml += '</dataVariable>\n'

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

        # Add the xml for the datasets
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

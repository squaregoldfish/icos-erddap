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
import html
import pandas as pd
from icoscp_core.icos import data
from Attribute import Attribute
from collections import defaultdict

_LOG_FILE_ = "populate_erddap.log"

_SPEC_TO_STRING = {
    "http://meta.icos-cp.eu/resources/cpmeta/icosOtcL2Product": "SOOP",
    "http://meta.icos-cp.eu/resources/cpmeta/icosOtcFosL2Product": "FOS"
}

_FOS_VAR_FILE = 'fos_variables.csv'
_SOOP_VAR_FILE = 'soop_variables.csv'

_AXIS_VARS = ['time', 'depth', 'longitude', 'latitude']

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
                            format="%(asctime)s %(levelname)s - %(message)s", level=config["log_level"])
        _check_config(config)

        logging.info("Checking for required ERDDAP updates")

        with database.connect() as db:
            datasets = database.get_pids_with_status(db)

            for dataset in datasets:
                dataset_ok = True

                # Download the new dataset if required
                if dataset["new"]:
                    logging.info(f"Downloading dataset {dataset['pid']}")
                    dataset_dir = os.path.join(config["datasets_dir"], dataset["pid"])
                    os.makedirs(dataset_dir, exist_ok=True)

                    try:
                        data.save_to_folder(icos.make_data_object_uri(dataset["pid"]), dataset_dir)
                        database.clear_new(db, dataset["pid"])
                    except Exception:
                        logging.error(f"Unable to download dataset {dataset['pid']}")
                        logging.error(traceback.format_exc())
                        dataset_ok = False

                if dataset_ok:
                    if dataset["updated"]:
                        try:
                            _make_datasets_xml_entry(db, dataset["pid"])
                        except Exception:
                            logging.error(f"Failed to build datasets.xml for dataset {dataset['pid']}")
                            logging.error(traceback.format_exc())

            _write_datasets_xml(config, db)
    except Exception as e:
        logging.critical(f"Unhandled exception: {e}")
        logging.critical(traceback.format_exc())
        exit(1)


def _make_datasets_xml_entry(db, pid):
    metadata = database.get_metadata(db, pid)

    entry = '<dataset type="EDDTableFromAsciiFiles" '
    entry += f'datasetID="{pid}" '
    entry += f'active="true"'
    entry += ">\n"

    spec = metadata["data_object"]["spec"].split("/")[-1]
    if spec == "icosOtcL2Product":
        entry += _make_soop_entry(pid, metadata, config)
    elif spec == "icosOtcFosL2Product":
        entry += _make_fos_entry(pid, metadata, config)
    else:
        raise ValueError(f"Unrecognised dobj spec {spec}")

    entry += "</dataset>\n"

    database.write_datasets_xml(db, pid, entry)


def _make_soop_entry(pid, metadata, config):
    # Common base details
    entry = _make_common_base(pid, config)

    # Attributes
    entry += "<addAttributes>\n"
    entry += _make_common_attributes(pid, metadata, "Trajectory")
    entry += _make_attribute_xml(Attribute("cdm_trajectory_variables", "expocode"))

    entry += _make_people_xml(metadata)

    # Make attributes from metadata in SQLite. Need a config file to
    # (a?) Map metadata entries to attribute names.
    # (b) Determine which attributes we copy, and which we don't.
    # (C) What about multiple links, i.e. to sensors? See what we get and decide how to deal with it.
    #
    # In theory this should be the same for both FOS and SOOP - the metadata in SQLite will have
    # everything and we just need to add it if it's there. Some will be TOP LEVEL and some will be NOT ADDED,
    # which need to be configured.
    entry += "</addAttributes>\n"

    # Data Variables
    entry += _generate_data_variables_xml(_SOOP_VAR_FILE)
    entry += _make_expocode_variable("trajectory_id")

    return entry


def _make_people_xml(metadata):
    xml = ""
    if "people" in metadata:
        entries = metadata["people"]

        entry_index = 1
        for person in entries:
            for key in ["id", "title", "firstName", "middleName", "lastName", "email", "orcid"]:
                xml += _make_attribute_xml(Attribute(f"person_{entry_index}_{key}", person[key]))

            org_names = (org["name"] for org in person["orgs"])
            xml += _make_attribute_xml(Attribute(f"person_{entry_index}_org", ";".join(org_names)))

            entry_index = entry_index + 1
    return xml

def _make_fos_entry(pid, metadata, config):
    # Common base details
    entry = _make_common_base(pid, config)

    # Attributes
    entry += "<addAttributes>\n"
    entry += _make_common_attributes(pid, metadata, "TimeSeries")
    entry += _make_attribute_xml(Attribute("cdm_timeseries_variables", "expocode"))
    entry += _make_people_xml(metadata)
    entry += "</addAttributes>\n"

    # Data Variables
    entry += _generate_data_variables_xml(_FOS_VAR_FILE)
    entry += _make_expocode_variable("timeseries_id")

    return entry


def _make_common_base(pid, config):
    base = "<reloadEveryNMinutes>10080</reloadEveryNMinutes>\n"
    base += "<updateEveryNMillis>10000</updateEveryNMillis>\n"
    base += f"<fileDir>{html.escape(os.path.join(config['datasets_dir'], pid))}</fileDir>\n"
    base += "<fileNameRegex>.*\\.csv</fileNameRegex>\n"
    base += "<recursive>false</recursive>\n"
    base += "<pathRegex>.*</pathRegex>\n"
    base += "<metadataFrom>last</metadataFrom>\n"
    base += "<standardizeWhat>0</standardizeWhat>\n"
    base += "<charset>UTF-8</charset>\n"
    base += "<columnSeparator>,</columnSeparator>\n"
    base += "<columnNamesRow>1</columnNamesRow>\n"
    base += "<firstDataRow>2</firstDataRow>\n"
    base += "<sortedColumnSourceName>Date/Time</sortedColumnSourceName>\n"
    base += "<sortFilesBySourceNames>Date/Time</sortFilesBySourceNames>\n"
    base += "<fileTableInMemory>false</fileTableInMemory>\n"
    return base


def _make_common_attributes(pid, metadata, cdm_data_type):
    common_attributes = _make_attribute_xml(Attribute("cdm_data_type", cdm_data_type))
    common_attributes += _make_attribute_xml(Attribute("Conventions", 'COARDS, CF-1.6, ACDD-1.3'))
    common_attributes += _make_attribute_xml(Attribute("infoUrl", icos.make_data_object_uri(pid)))
    common_attributes += _make_attribute_xml(Attribute("institution",
                                                       f"ICOS RI; {html.escape(metadata['station']['responsibleOrgName'])}"))
    if "keywords" in metadata["data_object"].keys():
        common_attributes += _make_attribute_xml(Attribute("keywords", html.escape(metadata["data_object"]["keywords"])))

    common_attributes += _make_attribute_xml(Attribute("license",
                                                       "CC BY 4.0/ICOS Data Licence: https://www.icos-cp.eu/data-services/about-data-portal/data-license"))
    common_attributes += _make_attribute_xml(Attribute("sourceUrl", icos.make_data_object_uri(pid)))
    common_attributes += _make_attribute_xml(Attribute("standard_name_vocabulary", "CF Standard Name Table v70"))
    common_attributes += _make_attribute_xml(Attribute("summary",
                                                       f"ICOS OTC {_SPEC_TO_STRING[metadata['data_object']['spec']]} Release from {html.escape(metadata['station']['name'])}"))
    common_attributes += _make_attribute_xml(
        Attribute("title", f"{html.escape(metadata['station']['name'])} - {html.escape(metadata['data_object']['expocode'])}"))
    common_attributes += _make_attribute_xml(
        Attribute("citation", f"{html.escape(metadata['data_object']['citation'])}"))

    return common_attributes


def _generate_data_variables_xml(var_file):
    xml = ""

    with open(f"erddap/{var_file}") as var_in:
        # Skip the first line - it's a header
        var_in.readline()

        fields = var_in.readline().split(",")
        while len(fields) > 1:
            attributes = list()
            if len(fields[COLOUR_BAR_MIN_COL]) > 0:
                attributes.append(Attribute("colorBarMinimum", fields[COLOUR_BAR_MIN_COL], "float"))
            if len(fields[COLOUR_BAR_MAX_COL]) > 0:
                attributes.append(Attribute("colorBarMaximum", fields[COLOUR_BAR_MAX_COL], "float"))
            if len(fields[FILL_VALUE_COL]) > 0:
                attributes.append(Attribute("_FillValue", fields[FILL_VALUE_COL]))

            if len(fields[OTHER_ATTRIBUTES_COL].strip()) > 0:
                other_attrs = fields[OTHER_ATTRIBUTES_COL].strip().split(";")
                for attr in other_attrs:
                    m = re.search("(.*):(.*)=(.*)", attr)
                    if not m:
                        raise ValueError(f"Invalid attribute specification '{attr}'")
                    attr_name, attr_type, attr_value = m.group(1, 2, 3)
                    if len(attr_type) == 0:
                        attributes.append(Attribute(attr_name, attr_value))
                    else:
                        attributes.append(Attribute(attr_name, attr_value, attr_type))

            xml += _make_data_variable_xml(fields[SOURCE_NAME_COL], fields[DESTINATION_COL], fields[DATA_TYPE_COL],
                                           fields[UNITS_COL], fields[IOOS_CATEGORY_COL], fields[LONG_NAME_COL],
                                           fields[STANDARD_NAME_COL], attributes)

            fields = var_in.readline().split(",")

    return xml


def _make_data_variable_xml(source_name, destination_name, data_type, units, ioos_category, long_name,
                            standard_name, other_attributes):
    xml = "<dataVariable>\n"
    xml += f"<sourceName>{html.escape(source_name)}</sourceName>\n"
    xml += f"<destinationName>{html.escape(destination_name)}</destinationName>\n"
    xml += f"<dataType>{data_type}</dataType>\n"

    xml += "<addAttributes>\n"
    xml += _make_attribute_xml(Attribute("units", units))
    xml += _make_attribute_xml(Attribute("ioos_category", ioos_category))
    xml += _make_attribute_xml(Attribute("source_name", source_name))
    xml += _make_attribute_xml(Attribute("long_name", long_name))
    if standard_name is not None:
        xml += _make_attribute_xml(Attribute("standard_name", standard_name))

    for attribute in other_attributes:
        xml += _make_attribute_xml(attribute)

    xml += "</addAttributes>\n"

    xml += "</dataVariable>\n"

    return xml


def _make_attribute_xml(attribute):
    attr = f'<att name="{attribute.name}"'
    if attribute.attr_type is not None:
        attr += f' type="{attribute.attr_type}"'
    attr += f">{attribute.value}</att>\n"

    return attr


def _make_expocode_variable(cf_role=None):
    xml = "<dataVariable>\n"
    xml += f"<sourceName>***fileName,(.*)\\.csv,1</sourceName>\n"
    xml += f"<destinationName>expocode</destinationName>\n"
    xml += "<dataType>String</dataType>\n"

    xml += "<addAttributes>\n"
    xml += _make_attribute_xml(Attribute("ioos_category", "Unknown"))
    xml += _make_attribute_xml(Attribute("long_name", "EXPOCODE"))

    if cf_role is not None:
        xml += _make_attribute_xml(Attribute("cf_role", cf_role))

    xml += "</addAttributes>\n"

    xml += "</dataVariable>\n"

    return xml

def make_axis_variable(name, values):
    xml = '<axisVariable>\n'
    xml += f'<sourceName>{name}</sourceName>\n'
    xml += f'<destinationName>{name}</destinationName>\n'
    xml += '<addAttributes>\n'
    xml += '<att name="axisValues" type="doubleList">'
    xml += ','.join([str(v) for v in values])
    xml += '</att>\n'
    xml += '</addAttributes>\n'
    xml += '</axisVariable>\n'

    return xml

def make_gridded_var(var):
    xml = '<dataVariable>\n'
    xml += f'<sourceName>{var}</sourceName>\n'
    xml += f'<destinationName>{var}</destinationName>\n'
    xml += '<addAttributes/>\n'
    xml += '</dataVariable>\n'

    return xml


def _write_datasets_xml(config, db):
    with open(config["datasets_xml_location"], "w") as out:
        # Write the erddap preamble and open root node
        out.write('<?xml version="1.0" encoding="UTF-8" ?>\n')
        out.write("<erddapDatasets>\n")

        # Copy the contents of the start template into the file
        with open("erddap/datasets_xml_start.xml", "r") as start:
            out.write(start.read())

        # Add the xml for the datasets
        datasets_xml = database.get_datasets_xml(db)
        for id in datasets_xml.keys():
            logging.debug(f"Adding datasets.xml for {id}")
            metadata = database.get_metadata(db, id)
            main_dataset_xml = datasets_xml[id]['xml']

            if datasets_xml[id]['deleted']:
                main_dataset_xml.replace('active="true"', 'active="false"')

            # Build the gridded XML entry
            gridded_xml = '<dataset type="EDDGridFromEDDTable" datasetID="'
            gridded_xml += f'{id}--gridded'
            gridded_xml += '" active="'
            gridded_xml += 'false' if datasets_xml[id]['deleted'] else 'true'
            gridded_xml += '">\n'

            with open("erddap/gridded_start.xml", "r") as gridded_start:
                gridded_xml += gridded_start.read()


            vars_file = None
            spec = _SPEC_TO_STRING[metadata['data_object']['spec']]
            if spec == 'SOOP':
                vars_file = _SOOP_VAR_FILE
            elif spec == 'FOS':
                vars_file = _FOS_VAR_FILE

            if vars_file is None:
                raise ValueError(f'Unrecognised spec {metadata["spec"]}')

            vars = list()
            with open(f'erddap/{vars_file}') as v:
                # Skip header
                v.readline()

                line = v.readline()
                while line is not None and len(line) > 0:
                    vars.append(line.split(',')[1])
                    line = v.readline()

            # Load the data
            filename = database.get_filename(db, id)
            dataset_dir = os.path.join(config["datasets_dir"], id)

            # This will need to change when we handle non-time-based data
            types = defaultdict(lambda: 'object', {'Longitude': 'float', 'Latitude': 'float'})
            df = pd.read_csv(os.path.join(dataset_dir, filename), index_col='Date/Time', dtype=types)
            df.index.names = ['time']
            df.index = pd.to_datetime(df.index).tz_localize(None)

            times = [int(x.timestamp()) for x in df.index.unique()]
            gridded_xml += make_axis_variable('time', sorted(set(times)))

            gridded_xml += make_axis_variable('longitude', sorted(df['Longitude'].dropna().unique()))
            gridded_xml += make_axis_variable('latitude', sorted(df['Latitude'].dropna().unique()))

            if 'depth' in vars and 'Depth [m]' in df.columns:
                gridded_xml += make_axis_variable('depth', df['Depth [m]'].unique())

            for var in vars:
                if var not in _AXIS_VARS:
                    gridded_xml += make_gridded_var(var)

            # Add the main dataset XML
            gridded_xml += main_dataset_xml

            gridded_xml += '</dataset>\n'

            out.write(gridded_xml)

            # Now write the main dataset
            out.write(main_dataset_xml)

        # Copy the contents of the end template into the file
        with open("erddap/datasets_xml_end.xml", "r") as end:
            out.write(end.read())

        # Close the root node
        out.write("</erddapDatasets>\n")


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

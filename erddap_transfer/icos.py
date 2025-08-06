"""
Functions for communicating with the ICOS Carbon Portal
"""
import pause
import logging
from dateutil.parser import isoparse
from datetime import datetime, timedelta
from icoscp_core.icos import meta
import pandas as pd
import os

# The ICOS Data Object Specs we're interested in
_DATA_TYPES = """
    <http://meta.icos-cp.eu/resources/cpmeta/icosOtcL2Product>
    <http://meta.icos-cp.eu/resources/cpmeta/icosOtcFosL2Product>
"""

_QUERY_PREFIX = """prefix cpmeta: <http://meta.icos-cp.eu/ontologies/cpmeta/>
                 prefix otcmeta: <http://meta.icos-cp.eu/ontologies/otcmeta/>
                 prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                 prefix prov: <http://www.w3.org/ns/prov#>
                 prefix xsd: <http://www.w3.org/2001/XMLSchema#>"""

_CP_PID_PREFIX = "https://meta.icos-cp.eu/objects/"

_OTC_STATION_ID_PREFIX = "http://meta.icos-cp.eu/resources/otcmeta/"

_OTC_STATION_ID_CACHE = dict()

_NEXT_QUERY_TIME = None

def get_all_data_object_ids():
    """
    Get the Data Objects that we're interested in, with start and end dates.

    The start time is rounded down to the start of the day, and the end time is rounded up to the next day.
    """
    logging.debug("Collecting data object IDs and start/end dates")

    query = f"""{_QUERY_PREFIX}
    SELECT ?dobj ?fileName ?timeStart ?timeEnd WHERE {{
    VALUES ?spec {{ {_DATA_TYPES} }}
    ?dobj cpmeta:hasObjectSpec ?spec .
    ?dobj cpmeta:hasSizeInBytes ?size .
    ?dobj cpmeta:hasName ?fileName .
    ?dobj cpmeta:hasStartTime | (cpmeta:wasAcquiredBy / prov:startedAtTime) ?timeStart .
    ?dobj cpmeta:hasEndTime | (cpmeta:wasAcquiredBy / prov:endedAtTime) ?timeEnd .
    FILTER (!CONTAINS(str(?fileName), "SOCAT"))
    FILTER NOT EXISTS {{[] cpmeta:isNextVersionOf ?dobj}}
    }}
    """
    query_result = run_query(query)
    result = []

    for record in query_result.bindings:
        uri = record["dobj"].uri
        pid = uri.split("/")[-1]

        expocode = os.path.splitext(record["fileName"].value)[0]
        start_time = _round_down(isoparse(record["timeStart"].value))
        end_time = _round_up(isoparse(record["timeEnd"].value))

        result.append((pid, expocode, start_time, end_time))

    return result


def _round_down(time):
    return time.date()


def _round_up(time):
    return _round_down(time + timedelta(days=1))


def get_metadata(datasets):
    """
    Retrieve the complete metadata for a PID as a python dict.
    """
    metadata = dict()
    pid_values_list = ""
    for (pid, expocode, start_date, end_date) in datasets:
        metadata[pid] = dict()
        pid_values_list += f"<{make_data_object_uri(pid)}> "

    # Get the central data object data for all PIDs
    # This comes from the main CP metadata
    with open("queries/data_object.sparql") as qin:
        data_object_query = qin.read()

    data_object_query = data_object_query.replace("%%VALUES%%", pid_values_list)
    query_result = run_query(data_object_query)
    for record in query_result.bindings:
        pid = pid_from_uri(getattr(record["data_object"], "uri"))
        metadata[pid]["data_object"] = dict()

        for field in record.keys():
            if field != "data_object":
                if hasattr(record[field], "uri"):
                    metadata[pid]["data_object"][field] = getattr(record[field], "uri")
                else:
                    metadata[pid]["data_object"][field] = getattr(record[field], "value")

    for (pid, expocode, start_date, end_date) in datasets:
        with open("queries/otc_metadata.sparql") as qin:
            otc_metadata_query = qin.read()

        otc_metadata_query = (otc_metadata_query.
                              replace("%%STATION%%", _get_otc_station_id(metadata[pid]["data_object"]["stationId"])).
                              replace("%%START_DATE%%", start_date.strftime("%Y-%m-%d")).
                              replace("%%END_DATE%%", end_date.strftime("%Y-%m-%d")))

        logging.debug(f"Retrieving station metadata for {pid}")
        query_result = run_query(otc_metadata_query)

        records = list()

        for record in query_result.bindings:
            fields = list()
            for field in query_result.variable_names:
                if field in record:
                    if hasattr(record[field], "uri"):
                        fields.append(getattr(record[field], "uri"))
                    else:
                        fields.append(getattr(record[field], "value"))
                else:
                    fields.append("")

            records.append(fields)

        if len(records) == 0:
            logging.warning(f"No metadata returned for PID {pid}")
        else:

            dataframe = pd.DataFrame(records, columns=query_result.variable_names)

            station_metadata = dict()
            station_metadata["name"] = dataframe["stationName"][0]
            station_metadata["otcName"] = dataframe["stationLabel"][0]
            station_metadata["responsibleOrgId"] = dataframe["responsibleOrgId"][0]
            station_metadata["responsibleOrgName"] = dataframe["responsibleOrgName"][0]
            metadata[pid]["station"] = station_metadata

            people = list()
            for person_id in pd.unique(dataframe["personId"]):
                person_data = dataframe[dataframe["personId"] == person_id]

                person = dict()
                person["id"] = person_id
                person["title"] = person_data["title"].iloc[0]
                person["firstName"] = person_data["firstName"].iloc[0]
                person["middleName"] = person_data["middleName"].iloc[0]
                person["lastName"] = person_data["lastName"].iloc[0]
                person["email"] = person_data["email"].iloc[0]
                person["orcid"] = person_data["orcid"].iloc[0]

                person_orgs_list = list()
                for person_org_id in pd.unique(person_data["personRoleOrg"]):
                    person_orgs = person_data[person_data["personRoleOrg"] == person_org_id]

                    person_org = dict()
                    person_org["id"] = person_org_id
                    person_org["name"] = person_orgs["personRoleOrgName"].iloc[0]
                    person_orgs_list.append(person_org)

                person["orgs"] = person_orgs_list

                people.append(person)

            metadata[pid]["people"] = people

            platform = dict()
            platform["id"] = dataframe["platformId"][0]
            platform["name"] = dataframe["platformName"][0]
            platform["code"] = dataframe["platformCode"][0]
            platform["deploymentSchedule"] = dataframe["deploymentSchedule"][0]
            platform["discreteSamplingSchedule"] = dataframe["discreteSamplingSchedule"][0]
            platform["instrumentSetup"] = dataframe["instrumentSetup"][0]
            platform["retrievalMethod"] = dataframe["retrievalMethod"][0]
            platform["airIntakePosition"] = dataframe["airIntakePosition"][0]
            platform["exhaustPosition"] = dataframe["exhaustPosition"][0]
            platform["portOfCall"] = dataframe["portOfCall"][0]
            platform["owner"] = dataframe["platformOwner"][0]

            metadata[pid]["platform"] = platform

            instruments = list()
            for instrument_id in pd.unique(dataframe["instrumentId"]):
                instrument = dict()

                instrument_data = dataframe[dataframe["instrumentId"] == instrument_id]

                instrument["id"] = instrument_id
                instrument["deviceId"] = instrument_data["instrumentDeviceId"].iloc[0]
                instrument["manufacturer"] = instrument_data["instrumentManufacturer"].iloc[0]
                instrument["model"] = instrument_data["instrumentModel"].iloc[0]
                instrument["serialNumber"] = instrument_data["instrumentSerialNumber"].iloc[0]
                instrument["documentationReference"] = instrument_data["instrumentDocumentationReference"].iloc[0]
                instrument["documentationComment"] = instrument_data["instrumentDocumentationComment"].iloc[0]
                instrument["samplingFrequency"] = instrument_data["instrumentSamplingFrequency"].iloc[0]
                instrument["reportingFrequency"] = instrument_data["instrumentReportingFrequency"].iloc[0]
                instrument["deviceSkos"] = instrument_data["instrumentDeviceSkos"].iloc[0]

                instrument_measures = pd.unique(instrument_data["instrumentMeasuresSkos"])
                if len(instrument_measures) == 1 and instrument_measures[0] == "":
                    instrument["measures"] = list()
                else:
                    instrument["measures"] = instrument_measures.tolist()

                instruments.append(instrument)

            metadata[pid]["instruments"] = instruments

            sensors = list()
            for sensor_id in pd.unique(dataframe["sensorId"]):
                sensor = dict()

                sensor_data = dataframe[dataframe["sensorId"] == sensor_id]

                sensor["id"] = sensor_id
                sensor["manufacturer"] = sensor_data["sensorManufacturer"].iloc[0]
                sensor["model"] = sensor_data["sensorModel"].iloc[0]
                sensor["serialNumber"] = sensor_data["sensorSerialNumber"].iloc[0]
                sensor["samplingFrequency"] = sensor_data["sensorSamplingFrequency"].iloc[0]
                sensor["reportingFrequency"] = sensor_data["sensorReportingFrequency"].iloc[0]
                sensor["deviceSkos"] = sensor_data["sensorDeviceSkos"].iloc[0]

                variables = list()
                for variable_id in pd.unique(sensor_data["variableId"]):
                    if variable_id != "":
                        variable = dict()

                        variable_data = sensor_data[sensor_data["variableId"] == variable_id]

                        variable["id"] = variable_id
                        variable["name"] = variable_data["variableName"].iloc[0]
                        variable["skosMatch"] = variable_data["variableSkosMatch"].iloc[0]

                        variables.append(variable)

                sensor["variables"] = variables

                sensors.append(sensor)

            metadata[pid]["sensors"] = sensors

    return metadata


def _get_otc_station_id(cp_station_id):
    if cp_station_id in _OTC_STATION_ID_CACHE:
        return _OTC_STATION_ID_CACHE[cp_station_id]
    else:
        query = f"{_QUERY_PREFIX}\n"
        query += f"select * where {{ <{cp_station_id}> cpmeta:hasOtcId ?otcId }}"

        query_result = run_query(query)
        _OTC_STATION_ID_CACHE[cp_station_id] = f"{_OTC_STATION_ID_PREFIX}{query_result.bindings[0]['otcId'].value}"
        return _OTC_STATION_ID_CACHE[cp_station_id]


def make_data_object_uri(pid):
    return f"{_CP_PID_PREFIX}{pid}"


def pid_from_uri(uri):
    return uri.split("/")[-1]


def run_query(query):
    global _NEXT_QUERY_TIME

    if _NEXT_QUERY_TIME is not None:
        if datetime.now() < _NEXT_QUERY_TIME:
            logging.debug(f"Query Pausing Until {_NEXT_QUERY_TIME}")
            pause.until(_NEXT_QUERY_TIME)

    logging.debug(f"Running Query:\n{query}")
    query_start = datetime.now()
    query_result = meta.sparql_select(query)
    query_end = datetime.now()

    query_time = query_end - query_start
    _NEXT_QUERY_TIME = datetime.now() + (query_time * 2)

    return query_result

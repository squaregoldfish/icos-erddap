"""
Functions for communicating with the ICOS Carbon Portal
"""
import logging
import json
from icoscp_core.icos import meta

# The ICOS Data Object Specs we're interested in
_DATA_TYPES = """
    <http://meta.icos-cp.eu/resources/cpmeta/icosOtcL2Product>
    <http://meta.icos-cp.eu/resources/cpmeta/icosOtcFosL2Product>
"""

_QUERY_PREFIX = """prefix cpmeta: <http://meta.icos-cp.eu/ontologies/cpmeta/>
                 prefix otcmeta: <http://meta.icos-cp.eu/ontologies/otcmeta/>
                 prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                 prefix prov: <http://www.w3.org/ns/prov#>"""

_CP_PID_PREFIX = "https://meta.icos-cp.eu/objects/"

_OTC_STATION_ID_PREFIX = "http://meta.icos-cp.eu/resources/otcmeta/"

_OTC_STATION_ID_CACHE = dict()


def get_all_data_object_ids():
    logging.debug("Collecting data object IDs")

    query = f"""{_QUERY_PREFIX}
    SELECT ?dobj WHERE {{
    VALUES ?spec {{ {_DATA_TYPES} }}
    ?dobj cpmeta:hasObjectSpec ?spec .
    ?dobj cpmeta:hasSizeInBytes ?size .
    ?dobj cpmeta:hasName ?fileName .
    FILTER (!CONTAINS(str(?fileName), "SOCAT"))
    FILTER NOT EXISTS {{[] cpmeta:isNextVersionOf ?dobj}}
    }}
    """

    logging.debug(f"Query:\n{query}")

    query_result = meta.sparql_select(query)
    result = []

    for record in query_result.bindings:
        uri = record["dobj"].uri
        pid = uri.split("/")[-1]
        result.append(pid)

    return result


def get_metadata(pids):
    """
    Retrieve the complete metadata for a PID as a python dict.
    The dict will be built in sections corresponding to the
    concepts and structures of the ICOS metadata.
    """
    metadata = dict()
    for pid in pids:
        metadata[pid] = {}

    # Get the central data object data
    _run_all_pids_metadata_query('data_object', metadata)

    for pid in pids:
        station_pid = _get_otc_station_id(metadata[pid]['data_object']['stationId'])
        _run_single_pid_metadata_query(pid, station_pid, 'station', metadata)

    return metadata


def _run_single_pid_metadata_query(parent_id, pid, subject, metadata):
    with open(f"query_fields/{subject}.json") as f:
        field_details = json.load(f)

    values_entry = f"<{pid}>"

    query = _build_query(subject, field_details, values_entry)

    query_result = meta.sparql_select(query)
    for record in query_result.bindings:
        item_data = _read_record(record, field_details)
        metadata[parent_id][subject] = item_data


def _run_all_pids_metadata_query(subject, metadata):
    with open(f"query_fields/{subject}.json") as f:
        field_details = json.load(f)

    values_entry = ""
    for pid in metadata.keys():
        values_entry += f"<{make_data_object_uri(pid)}> "

    query = _build_query(subject, field_details, values_entry)

    query_result = meta.sparql_select(query)
    for record in query_result.bindings:
        obj_id = pid_from_uri(getattr(record[subject], "uri"))
        metadata[obj_id][subject] = _read_record(record, field_details)


def _build_query(subject, field_details, values_entry):
    query = f"{_QUERY_PREFIX}\n"
    query += f"SELECT ?{subject}"

    for field in field_details['iris']:
        query += f" ?{field['object']}"

    if 'functions' in field_details:
        for func in field_details['functions']:
            query += f" ({func['function']} AS ?{func['object']})"

    query += " WHERE {\n"
    query += f"VALUES ?{subject} {{ {values_entry} }}\n"

    for field in field_details['iris']:
        if field['optional']:
            query += "OPTIONAL { "

        query += "?"
        if 'subject' in field:
            query += field['subject']
        else:
            query += subject

        query += f" {field['predicate']} ?{field['object']} ."

        if field['optional']:
            query += " }"

        query += "\n"

    query += "}"

    logging.debug(f"""Metadata query for {subject}:\n{query}""")
    return query


def _read_record(record, field_details):
    item_data = dict()

    for field in field_details['iris']:
        if field["object"] in record.keys():
            item_data[field["object"]] = getattr(record[field["object"]], field["type"])

    if 'functions' in field_details:
        for func in field_details['functions']:
            if func["object"] in record.keys():
                item_data[func["object"]] = getattr(record[func["object"]], func["type"])

    return item_data


def _get_otc_station_id(cp_station_id):
    if cp_station_id in _OTC_STATION_ID_CACHE:
        return _OTC_STATION_ID_CACHE[cp_station_id]
    else:
        query = f"{_QUERY_PREFIX}\n"
        query += f"select * where {{ <{cp_station_id}> cpmeta:hasOtcId ?otcId }}"

        query_result = meta.sparql_select(query)
        _OTC_STATION_ID_CACHE[cp_station_id] = f'{_OTC_STATION_ID_PREFIX}{query_result.bindings[0]["otcId"].value}'
        return _OTC_STATION_ID_CACHE[cp_station_id]


def make_data_object_uri(pid):
    return f"{_CP_PID_PREFIX}{pid}"


def pid_from_uri(uri):
    return uri.split("/")[-1]

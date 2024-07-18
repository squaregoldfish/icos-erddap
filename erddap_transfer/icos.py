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

_CP_QUERY_PREFIX = """prefix cpmeta: <http://meta.icos-cp.eu/ontologies/cpmeta/>
                 prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                 prefix prov: <http://www.w3.org/ns/prov#>"""


def get_all_data_object_ids():
    logging.debug("Collecting data object IDs")

    query = f"""{_CP_QUERY_PREFIX}
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
        id = uri.split("/")[-1]
        result.append(id)

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

    _run_metadata_query(_CP_QUERY_PREFIX, metadata, "data_object")

    return metadata


def _run_metadata_query(prefix, metadata, fields):
    with open(f"query_fields/{fields}.json") as f:
        field_details = json.load(f)

    query = f"{prefix}\n"
    query += "SELECT ?dobj ?station"

    for field in field_details:
        query += f" ?{field['name']}"

    query += " WHERE {\n"

    # Add all PIDs (as URIs)
    query += "VALUES ?dobj { "
    for pid in metadata.keys():
        query += f"<{make_data_object_uri(pid)}> "
    query += "}\n"

    # Station - used as the link to OTC metadata
    query += "?dobj cpmeta:wasAcquiredBy/prov:wasAssociatedWith ?station .\n"

    for field in field_details:
        if field['optional']:
            query += f"OPTIONAL {{ ?dobj {field['iri']} ?{field['name']} . }}\n"
        else:
            query += f"?dobj {field['iri']} ?{field['name']} .\n"

    query += "}"

    logging.debug(f"""Metadata query for {fields}:\n{query}""")

    query_result = meta.sparql_select(query)
    for record in query_result.bindings:
        obj_id = pid_from_uri(getattr(record["dobj"], "uri"))

        item_data = dict()

        for field in field_details:
            if field["name"] in record.keys():
                item_data[field["name"]] = getattr(record[field["name"]], field["type"])


        metadata[obj_id][fields] = item_data

        # The station is a special case - it's not meant to be part of the ERDDAP metadata
        metadata[obj_id]["station_uri"] = getattr(record["station"], "uri")


def make_data_object_uri(pid):
    return f"https://meta.icos-cp.eu/objects/{pid}"


def pid_from_uri(uri):
    return uri.split("/")[-1]

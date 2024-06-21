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
                 prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"""


def get_all_data_object_ids():
    logging.debug("Collecting data object IDs")

    query = f"""{_CP_QUERY_PREFIX}
    SELECT ?dobj WHERE {{
    VALUES ?spec {{ {_DATA_TYPES} }}
    ?dobj cpmeta:hasObjectSpec ?spec .
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


def get_metadata(pid):
    """
    Retrieve the complete metadata for a PID as a python dict.
    The dict will be built in sections corresponding to the
    concepts and structures of the ICOS metadata.
    """
    metadata = dict()

    metadata["data_object"] = _run_basic_query(_CP_QUERY_PREFIX, make_data_object_uri(pid), "data_object")

    return metadata


def _run_basic_query(prefix, item, fields):

    with open(f"query_fields/{fields}.json") as f:
        field_details = json.load(f)

    query = f"{prefix}\n"
    query += "SELECT ?obj"

    for field in field_details:
        query += f" ?{field['name']}"

    query += " WHERE {\n"
    query += f"VALUES ?obj {{ <{item}> }}\n"

    for field in field_details:
        query += f"?obj {field['iri']} ?{field['name']} .\n"

    query += "}"

    logging.debug(f"""Query for item {item}:\n{query}""")

    query_result = meta.sparql_select(query)
    result = dict()
    result["item"] = item

    for field in field_details:
        result[field["name"]] = getattr(query_result.bindings[0][field["name"]], field["type"])

    return result


def make_data_object_uri(pid):
    return f"https://meta.icos-cp.eu/objects/{pid}"

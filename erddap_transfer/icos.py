"""
Functions for communicating with the ICOS Carbon Portal
"""
import logging
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
        uri = record['dobj'].uri
        id = uri.split("/")[-1]
        result.append(id)

    return result
#!bin/python
"""
This is a simple Flask app that retrieves the CSV content for a data object from the Carbon Portal.
URLS for this app must be of the form http://localhost:5000/<pid>
The PID is same as that used in the URI for a Carbon Portal Data Object.

If the object exists, the CSV content will be returned. Otherwise a response
containing the status code and message from the underlying failure will be returned.
"""

import re
from flask import Flask
from icoscp_core.icos import data

app = Flask(__name__)

@app.route("/<pid>")
def get_csv(pid):
    """
    Retrieve the CSV data for the specified PID.
    The PID is as used in the URI for a data object.
    """
    result = None

    try:
        result = data.get_csv_byte_stream(pid)
    except Exception as e:
        # Parse the exception string
        regex = re.compile('.*HTTP response code: ([0-9]+), response: (.*)')
        match = regex.match(e.args[0])
        result = match[2], int(match[1]), []

    return result

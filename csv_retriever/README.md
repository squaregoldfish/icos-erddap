Flask script to retrieve CSV data from the Carbon Portal.

URLS for this app must be of the form `http://localhost:5000/<pid>`
The PID is same as that used in the URI for a Carbon Portal Data Object.

If the object exists, the CSV content will be returned. Otherwise a response
containing the status code and message from the underlying failure will be returned.


Required pip packages:
Flask
icoscp_core

import io
import pandas as pd
import requests
import sqlite3

"""
Find datasets that don't contain both a gridded and non-gridded entry.
Delete them from the transfer database so they get recreated.
"""

# Download dataset IDs
ALL_DATASETS_URL = 'https://erddap.icos-cp.eu/erddap/search/advanced.csv?page=1&itemsPerPage=1000000&searchFor=all'
response = requests.get(ALL_DATASETS_URL)
datasets = list(pd.read_csv(io.StringIO(response.text))['Dataset ID'])

# Split into gridded and non-gridded
gridded = sorted([x for x in datasets if x.endswith("--gridded")])
non_gridded = sorted([x for x in datasets if not x.endswith("--gridded") and x != 'allDatasets'])
gridded = [x[:-9] for x in gridded]

# Find elements not in both lists
unmatched = list()

g = 0
n = 0

while g < len(gridded) and n < len(non_gridded):

    if gridded[g] == non_gridded[n]:
        g += 1
        n += 1
    elif gridded[g] < non_gridded[n]:
        unmatched.append(gridded[g])
        g += 1
    else:
        unmatched.append(non_gridded[n])
        n += 1

while g < len(gridded):
    unmatched.append(gridded[g])
    g += 1

while n < len(non_gridded):
    unmatched.append(non_gridded[n])
    n += 1

with sqlite3.connect('erddap_transfer.sqlite') as db:
    for expocode in unmatched:
        db.execute('DELETE FROM data_object WHERE expocode = ?', ((expocode,)))

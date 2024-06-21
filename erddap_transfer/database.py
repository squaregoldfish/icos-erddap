"""
Functions to interact with the database that
tracks the status of the ERDDAP transfer mechanism.
"""
import sqlite3
import json
import logging
from datetime import datetime

_DB_FILE_ = "erddap_transfer.sqlite"


def connect():
    """
        Obtain a connection to the database. If the database doesn't exist, create it.
    """
    logging.debug("Connecting to database")

    db_conn = sqlite3.connect(_DB_FILE_)

    if not _is_db_set_up(db_conn):
        _init_db(db_conn)

    return db_conn


def is_in_db(conn, pid):
    """
    See if the specified data object ID is in the database
    """
    logging.debug(f"Checking database for PID {pid}")
    c = conn.cursor()
    try:
        c.execute("SELECT COUNT(*) FROM data_object WHERE id = ?", [pid])
        return True if c.fetchone()[0] > 0 else False
    finally:
        c.close()


def is_deleted(conn, pid):
    """
    Check the DELETED status of a specified record
    """
    logging.debug(f"Checking deleted status of {pid}")
    c = conn.cursor()
    try:
        c.execute("SELECT deleted FROM data_object WHERE id = ?", [pid])
        record = c.fetchone()
        if record is None:
            raise ValueError(f"PID {pid} not in database")
        else:
            return False if record[0] == 0 else True
    finally:
        c.close()


def undelete(conn, pid):
    """
    Clear the DELETED flag for a specified record.
    """
    logging.info(f"Removing delete flag for {pid}")
    c = conn.cursor()
    try:
        c.execute("UPDATE data_object SET deleted = 0, deleted_time = NULL, updated = 1 WHERE id = ?", [pid])
        if c.rowcount == 0:
            raise ValueError(f"PID {pid} not in database")
    finally:
        c.close()


def get_active_pids(conn):
    logging.debug("Retrieving all stored PIDs")
    c = conn.cursor()
    try:
        c.execute("SELECT id FROM data_object WHERE deleted = 0 ORDER BY id")
        records = c.fetchall()
        return [r[0] for r in records]
    finally:
        c.close()


def add_pid(conn, pid):
    """
    Add a new PID to the database with no metadata.
    The record has its "new" flag set.
    """
    logging.info(f"Adding {pid} to database")

    c = conn.cursor()
    try:
        c.execute("INSERT INTO data_object VALUES(?, NULL, NULL, 1, 0, 0, NULL)", [pid])
    finally:
        c.close()


def mark_deleted(conn, pid):
    logging.info(f"Marking {pid} as deleted")
    c = conn.cursor()
    try:
        c.execute("UPDATE data_object SET deleted = 1, deleted_time = ? WHERE id = ?",
                  [datetime.now().isoformat(), pid])
        if c.rowcount == 0:
            raise ValueError(f"PID {pid} not in database")
    finally:
        c.close()


def _is_db_set_up(conn):
    """
    Simple check to see if the database has been initialised.
    """
    c = conn.cursor()
    try:
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='data_object'")
        return len(c.fetchall()) > 0
    finally:
        c.close()


def get_metadata(conn, pid):
    """
    Get the metadata for a given pid
    """
    c = conn.cursor()
    try:
        c.execute("SELECT metadata FROM data_object WHERE id = ?", [pid])
        record = c.fetchone()
        if record is None:
            raise ValueError(f"PID {pid} not in database")

        return dict() if record[0] is None else json.loads(record[0])
    finally:
        c.close()


def set_metadata(conn, pid, metadata):
    """
    Store metadata for a PID
    """
    logging.info(f"Updating metadata for PID {pid}")
    c = conn.cursor()
    try:
        c.execute("UPDATE data_object SET metadata = ?, updated = 1 WHERE id = ?", [json.dumps(metadata), pid])
        if c.rowcount == 0:
            raise ValueError(f"PID {pid} not in database")
    finally:
        c.close()


def clear_new(conn, pid):
    """
    Set the NEW flag for a PID to False
    """
    logging.debug(f"Clearing NEW flag for {pid}")
    c = conn.cursor()
    try:
        c.execute("UPDATE data_object SET new = 0 WHERE id = ?", [pid])
        if c.rowcount == 0:
            raise ValueError(f"PID {pid} not in database")
    finally:
        c.close()

def get_pids_with_status(conn):
    c = conn.cursor()
    try:
        c.execute("SELECT id, new, updated, deleted FROM data_object")
        records = c.fetchall()
        result = []

        for record in records:
            dataset = dict()
            dataset["pid"] = record[0]
            dataset["new"] = bool(record[1])
            dataset["update"] = bool(record[2])
            dataset["delete"] = bool(record[3])

            result.append(dataset)

        return result
    finally:
        c.close()

def get_filename(conn, pid):
    """
    Get the filename for a PID
    """
    c = conn.cursor()
    try:
        c.execute("SELECT metadata FROM data_object WHERE id = ?", [pid])
        record = c.fetchone()
        if record is None:
            raise ValueError(f"PID {pid} not in database")

        metadata = json.loads(record[0])
        return metadata["data_object"]["fileName"]
    finally:
        c.close()

def _init_db(conn):
    logging.info("Initialising new database")

    """
    Initialise the database
    """
    table_sql = """CREATE TABLE data_object(
    id TEXT,
    metadata TEXT,
    datasets_xml TEXT,
    new INTEGER,
    updated INTEGER,
    deleted INTEGER,
    deleted_time TEXT,
    PRIMARY KEY (id)
    )"""

    c = conn.cursor()
    try:
        c.execute(table_sql)
    finally:
        c.close()

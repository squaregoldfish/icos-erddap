"""
Retrieve database information from the ICOS Carbon Portal
and update the local database ready for the ERDDAP
server to be updated
"""
import database
import icos
import logging
import toml
import traceback

_LOG_FILE_ = "update_from_icos.log"


def main(config):
    try:
        logging.basicConfig(filename=_LOG_FILE_,
                            format='%(asctime)s %(levelname)s - %(message)s', level=config["log_level"])
        logging.info("Updating info from ICOS Carbon Portal")

        # Connect to the internal database
        with database.connect() as db:

            # Get the list of all data objects in the Carbon Portal
            cp_pids = sorted(icos.get_all_data_object_ids())

            for pid in cp_pids:
                if not database.is_in_db(db, pid):
                    database.add_pid(db, pid)
                elif database.is_deleted(db, pid):
                    database.undelete(db, pid)


            # Get database IDs excluding deleted
            # If any not in cp_ids, mark as deleted
            local_pids = database.get_active_pids(db)
            newly_deleted_pids = list(set(local_pids) - set(cp_pids))
            for pid in newly_deleted_pids:
                database.mark_deleted(db, pid)


            # Get the metadata for the PIDs and update if necessary
            metadata = icos.get_metadata(cp_pids)

            for pid in metadata.keys():
                existing_metadata = database.get_metadata(db, pid)
                metadata_updated = existing_metadata is None or existing_metadata != metadata[pid]

                if metadata_updated:
                    database.set_metadata(db, pid, metadata[pid])

    except Exception as e:
        logging.error(f"Unhandled exception: {e}")
        logging.error(traceback.format_exc())


if __name__ == "__main__":
    with open("config.toml") as f:
        config = toml.load(f)

    main(config)

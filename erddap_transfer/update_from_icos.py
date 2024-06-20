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

        # Connect to the internal database
        with database.connect() as db:

            # Get the list of all data objects in the Carbon Portal
            cp_pids = sorted(icos.get_all_data_object_ids())

            for pid in cp_pids:
                if not database.is_in_db(db, pid):
                    database.add_pid(db, pid)
                elif database.is_deleted(db, pid):
                    database.undelete(db, pid)

                # Retrieve the metadata
                # If different/new, add it and set Updated flag (if not new)


            # Get database IDs excluding deleted
            # If any not in cp_ids, mark as deleted
            local_pids = database.get_active_pids(db)
            newly_deleted_pids = list(set(local_pids) - set(cp_pids))
            for pid in newly_deleted_pids:
                database.mark_deleted(db, pid)




    except Exception as e:
        logging.error(f"Unhandled exception: {e}")
        logging.error(traceback.format_exc())


if __name__ == "__main__":
    with open("config.toml") as f:
        config = toml.load(f)

    main(config)

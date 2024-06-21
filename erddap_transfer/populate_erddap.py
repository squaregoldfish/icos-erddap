"""
Set up data files and datasets.xml for an ERDDAP server
"""
import toml
import logging
import traceback
import database
import os
import icos
from icoscp_core.icos import data

_LOG_FILE_ = "populate_erddap.log"

def main(config):
    try:
        logging.basicConfig(filename=_LOG_FILE_,
                            format='%(asctime)s %(levelname)s - %(message)s', level=config["log_level"])
        _check_config(config)

        logging.info("Checking for required ERDDAP updates")

        with database.connect() as db:
            datasets = database.get_pids_with_status(db)

            for dataset in datasets:
                dataset_ok = True

                # Download the new dataset if required
                if dataset["new"]:
                    logging.info(f"Downloading dataset {dataset["pid"]}")
                    dataset_dir = os.path.join(config["datasets_dir"], dataset["pid"])
                    os.makedirs(dataset_dir, exist_ok = True)

                    try:
                        data.save_to_folder(icos.make_data_object_uri(dataset["pid"]), dataset_dir)
                        database.clear_new(db, dataset["pid"])
                    except Exception as e:
                        logging.error(f"Unable to download dataset {dataset["pid"]}")
                        logging.error(traceback.format_exc())
                        dataset_ok = False






    except Exception as e:
        logging.critical(f"Unhandled exception: {e}")
        logging.critical(traceback.format_exc())


def _check_config(config):
    try:
        os.makedirs(config["datasets_dir"], exist_ok = True)
    except Exception:
        logging.critical("Cannot create datasets directory")
        logging.critical(traceback.format_exc())
        exit(1)


if __name__ == "__main__":
    with open("config.toml") as f:
        config = toml.load(f)

    main(config)



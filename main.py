import logging
import os
import sys

from logging_config import setup_logging

setup_logging()

import data.data_config as data_cfg
from data.fetch_data import DB_Handler


def main():
    logger = logging.getLogger(__name__)
    logger.info("Application started")

    db_handler = DB_Handler(data_cfg.DB, data_cfg.TICKERS)
    # db_handler.drop_all_tables()
    db_handler.create_tables()
    db_handler.insert_history()
    db_handler.commit_and_close()

    logger.info("Application finished")
    return 0


if __name__ == "__main__":
    sys.exit(main())

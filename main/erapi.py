# -*- encoding: utf-8 -*-

"""
A Script to Update Data for ExchangeRatesIO on MacroDB Schema

Given a database schema with keys and other definitions intact as in
https://github.com/aivenio/macrodb, the query can be used to update
the data using CRON schedulers, or GitHub actions.
"""

import os # operating system functionalities

from config import setLogger

if __name__ == "__main__":
    key = os.getenv("dummy", None)
    logger = setLogger(name = "ERAPI")

    logger.info(f"Loaded Key: {key}")

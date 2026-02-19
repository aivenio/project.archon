# -*- encoding: utf-8 -*-

"""
A Script to Update Data for ExchangeRatesIO on MacroDB Schema

Given a database schema with keys and other definitions intact as in
https://github.com/aivenio/macrodb, the query can be used to update
the data using CRON schedulers, or GitHub actions.
"""

import os
import sys

import sqlalchemy as sa

# append additional files, check actions for more information
sys.path.append(
    os.path.join(os.path.dirname(__file__), "..")
)
sys.path.append("dtutils")

import forexrates # get the module from repository root

# https://ds-gringotts.readthedocs.io/en/latest/modules/utils/dtutils.html
import datetime_ as dt_ # cloned using git, ./dtutils

from utils import getDates
from config import setLogger
from config import createEngine

if __name__ == "__main__":
    API_KEY = os.environ["EXCHANGERATES_IO_API_KEY"]
    
    # create a logger for the erapi module
    logger = setLogger(name = "ERAPI")

    # get configurations for database connection elements, and build
    DATABASE = os.environ["AIVENIO_MACRODB_DATABASE"]
    HOSTNAME = os.environ["AIVENIO_MACRODB_HOSTNAME"]
    PASSWORD = os.environ["AIVENIO_MACRODB_PASSWORD"]
    PORTNAME = os.environ["AIVENIO_MACRODB_PORTNAME"]
    USERNAME = os.environ["AIVENIO_MACRODB_USERNAME"]

    engine = createEngine(
        host = HOSTNAME, port = PORTNAME,
        user = USERNAME, password = PASSWORD,
        database = DATABASE, logger = logger, verbose = False
    )


    # use the utility function to get the dates, log information
    dates = getDates(engine = engine, logger = logger)

    # use the forexrates module to fetch the data from the api
    data = [
        forexrates.api.ExchangeRatesAPI(
            apikey = API_KEY, endpoint = date.strftime("%Y-%m-%d")
        ).get(
            verify = False, suppresswarning = True
        ) for date in dates
    ]

    parser = forexrates.io.dataframe.ExchangeRatesIO(data)
    dataframe = parser.dataframe(
        index = "exchange_rate", verbose = True
    )
    dataframe["data_source_id"] = "ERAPI"

    with engine.connect() as connection:
        metadata = sa.Table(
            "forex_rate_tx", sa.MetaData(schema = "common"),
            autoload_with = connection
        )

        connection.execute(metadata.insert(), dataframe.to_dict(orient = "records"))
        connection.commit()

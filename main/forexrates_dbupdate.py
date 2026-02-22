# -*- encoding: utf-8 -*-

"""
A Script to Update Data for ExchangeRatesIO on MacroDB Schema

Given a database schema with keys and other definitions intact as in
https://github.com/aivenio/macrodb, the query can be used to update
the data using CRON schedulers, or GitHub actions.
"""

import os
import sys

import logging
import datetime as dt
import sqlalchemy as sa

from typing import List

# append additional files, check forexrates actions for more information
sys.path.append("dtutils")

# https://ds-gringotts.readthedocs.io/en/latest/modules/utils/dtutils.html
import datetime_ as dt_ # cloned using git, ./dtutils

import forexrates # get the module from repository root

from config import set_logger
from config import DatabaseController

def get_dates(controller : object, logger : object) -> List[dt.date]:
    """
    Get date range on which the :mod:`forexrates` iterate to fetch
    the result, this uses custom library, check module documentation
    for more details.

    :type  controller: object
    :param controller: Database controller object, that can be used to
        do CRUD operations. Check config module for more information.

    :type  logger: object
    :param logger: Instance of logger object to log into a specified
        file, check logger configuration in documentation.
    """

    start = controller.get_data(
        query = "SELECT MAX(effective_date) FROM common.forex_rate_tx",
        fetchone = True, useexecute = True
    )

    end =  dt.datetime.now().date() - dt.timedelta(days = 1)
    
    start += dt.timedelta(days = 1)
    dates = list(dt_.date_range(start = start, end = end))

    logger.info(f"Trying to fetch data from {start} to {end}.")
    logger.info(f"This will consume {len(dates):,} API calls.")

    return dates


if __name__ == "__main__":
    API_KEY = os.environ["EXCHANGERATES_IO_API_KEY"]
    
    # create a logger for the erapi module
    set_logger(
        configfile = "./config/logging.yaml",
        outfile = "./logs/forexrates.log"
    )

    forexlogger = logging.getLogger("FOREX Rates Logger")

    dbobject = DatabaseController(
        hostname = os.environ["AIVENIO_MACRODB_HOSTNAME"],
        portname = os.environ["AIVENIO_MACRODB_PORTNAME"],
        username = os.environ["AIVENIO_MACRODB_USERNAME"],
        password = os.environ["AIVENIO_MACRODB_PASSWORD"],
        database = os.environ["AIVENIO_MACRODB_DATABASE"]
    )

    # use the utility function to get the dates, log information
    dates = get_dates(engine = dbobject, logger = forexlogger)

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
    dbobject.insert_data(
        dataframe = dataframe,
        tablename = "forex_rate_tx",
        schemaname = "common"
    )

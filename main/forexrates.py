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
from config import create_engine

def get_dates(
        engine : sa.Engine,
        logger : object,
        tablename : str = "common.forex_rate_tx",
        datecolumn : str = "effective_date"
    ) -> List[dt.date]:
    """
    Get date range on which the :mod:`forexrates` iterate to fetch
    the result, this uses custom library, check module documentation
    for more details.

    :type  engine: sa.Engine
    :param engine: Instance of SQLAlchemy engine object, or compatible
        objects that can be called like ``.connect()`` using a
        context manager. The function is tested with SQLAlchemy, and
        other libraries are not checked.

    :type  logger: object
    :param logger: Instance of logger object to log into a specified
        file, check logger configuration in documentation.

    :type  tablename: str
    :param tablename: Table name from which the last available date
        is to be fetched, defaults to MacroDB schema design (check
        https://github.com/aivenio/macrodb) - ``common.forex_rate_tx``.

    :type  datecolumn: str
    :para, datecolumn: Date column in the table for which the last
        available day is fetched, defaults to ``effective_date``.
    """

    statement = f"SELECT MAX({datecolumn}) FROM {tablename}"

    with engine.connect() as connection:
        start = connection.execute(sa.text(statement)).fetchone()[0]

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

    # get configurations for database connection elements, and build
    DATABASE = os.environ["AIVENIO_MACRODB_DATABASE"]
    HOSTNAME = os.environ["AIVENIO_MACRODB_HOSTNAME"]
    PASSWORD = os.environ["AIVENIO_MACRODB_PASSWORD"]
    PORTNAME = os.environ["AIVENIO_MACRODB_PORTNAME"]
    USERNAME = os.environ["AIVENIO_MACRODB_USERNAME"]

    engine = create_engine(
        host = HOSTNAME, port = PORTNAME,
        user = USERNAME, password = PASSWORD,
        database = DATABASE, verbose = False
    )


    # use the utility function to get the dates, log information
    dates = get_dates(engine = engine, logger = forexlogger)

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

        connection.execute(
            metadata.insert(),
            dataframe.to_dict(orient = "records")
        )
        connection.commit()

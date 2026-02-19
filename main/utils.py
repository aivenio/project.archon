# -*- encoding: utf-8 -*-

"""
Utility Function(s) for CI/CD Pipelines under Main Directory
"""

import datetime as dt
import sqlalchemy as sa

from typing import List

import datetime_ as dt_

def getDates(
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

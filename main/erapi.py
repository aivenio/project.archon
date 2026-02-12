# -*- encoding: utf-8 -*-

"""
A Script to Update Data for ExchangeRatesIO on MacroDB Schema

Given a database schema with keys and other definitions intact as in
https://github.com/aivenio/macrodb, the query can be used to update
the data using CRON schedulers, or GitHub actions.
"""

import os
import sys

import pandas as pd
import datetime as dt
import sqlalchemy as sa

# append additional files, check actions for more information
sys.path.append("..")
sys.path.append("dtutils")

import forexrates # get the module from repository root

# https://ds-gringotts.readthedocs.io/en/latest/modules/utils/dtutils.html
import datetime_ as dt_ # cloned using git, ./dtutils

from config import setLogger # already exists in the current path

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

    logger.info(f"DB: {DATABASE}")

    # engine = sa.create_engine(
    #     f"postgresql://{USERNAME}:{PASSWORD}@{DATABASE}:{PORTNAME}/{DATABASE}"
    # )

    # try:
    #     engine.connect()
    # except Exception as err:
    #     logger.critical(f"Cannot connect to database. Error: {err}")
    # else:
    #     logger.info("Connection Established to MacroDB for AivenIO")

    # # get the last available date from the database, and then set a
    # # context to fetch the date period for which data is required
    # statement = """
    #     SELECT MAX(effective_date)::DATE AS last_date
    #     FROM common.forex_rate_tx
    # """

    # start_date = [
    #     dict(row) for row in engine.execute(statement)
    # ][0]["last_date"] + dt.timedelta(days = 1) # set to the next date

    # last_date = dt.datetime.now().date() - dt.timedelta(days = 1)
    # dates = list(dt_.date_range(start = start_date, end = last_date))

    # logger.info(f"Trying to fetch data from {start_date} to {last_date}")
    # logger.info(f"This will consume {len(dates):,} calls for the API")

    # data = [
    #     forexrates.api.ExchangeRatesAPI(
    #         apikey = API_KEY, endpoint = date.strftime("%Y-%m-%d")
    #     ).get(
    #         verify = False, suppresswarning = True
    #     ) for date in dates
    # ]

    # parser = forexrates.io.dataframe.ExchangeRatesIO(data)
    # dataframe = parser.dataframe(
    #     index = "exchange_rate", verbose = True
    # )
    # dataframe["data_source_id"] = "ERAPI"
    # dataframe.to_sql(
    #     "forex_rate_tx", engine, schema = "common", if_exists = "append", index = False
    # )

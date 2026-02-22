# -*- encoding: utf-8 -*-

"""
A Set of Configuration Function(s) for Main Files
"""

import os
import yaml
import logging
import logging.config

import pandas as pd
import sqlalchemy as sa

from typing import Any

def set_logger(configfile : str, outfile : str = None) -> None:
    """
    Setup a logger with pre-built configurations that can be used and
    extended by any endpoints to capture data update, deletion and
    other maintenance logs, using the :mod:`logger` module.

    :type  configfile: str
    :param configfile: Path to config file, this function uses PyYAML
        configuration file, check documentation for more information.

    :type  outfile: str
    :param outfile: Path to output log file, if not provided, the
        default configuration file is used.
    """

    config = yaml.safe_load(open(configfile, "r").read())

    # ? override output logging file if required
    if outfile:
        config["handlers"]["file"]["filename"] = outfile

    # ? override outfile variable, also can fetch from configfile
    outfile = config["handlers"]["file"]["filename"]

    if not os.path.exists(outfile):
        print(f"Creating Log File: {outfile}")
        open(outfile, "w", encoding = "utf-8").close() # write blank file

    logging.config.dictConfig(config)
    return


class DatabaseController:
    """
    Database Configuration Controller Class for CRUD Operations

    The database configuration that creates the :mod:`SQLAlchemy`
    engine that is compatible with context managers operations and
    can be used module wide to do operations on the database.

    :type  sadialect: str
    :param sadialect: SQLAlchemy dialects to establish connection to
        the database, https://docs.sqlalchemy.org/en/14/dialects/.
    """

    def __init__(
        self,
        hostname : str,
        portname : str | int,
        username : str,
        password : str,
        database : str,
        sadialect : str = "postgresql+psycopg"
    ) -> None:
        self.engine = self.__create_engine__(
            hostname = hostname,
            portname = portname,
            username = username,
            password = password,
            database = database,
            sadialect = sadialect
        )


    def get_data(
        self,
        query : str,
        fetchone : bool = False,
        useexecute : bool = False
    ) -> Any:
        """
        Get Data from the Database using the provided Query

        A simple function to get the data from the database using
        the provided query, and return the result. The function can
        be used in two-ways, either by using :mod:`pandas.read_sql` or
        by using :func:`execute` method of :mod:`sqlalchemy.engine`
        inside a context manager.

        :type  query: str
        :param query: Query to be executed on the database, check
            https://docs.sqlalchemy.org/en/14/core/connections.html.

        :type  fetchone: bool
        :param fetchone: Return a single row from the database (default
            is False), else uses ``fetchall`` method when True. When
            ``useexecute`` is True, this parameter uses connection
            method ``.fetch*()`` based on input, else :mod:`pandas`
            is used the method return ``.head(1)`` value.

        :type  useexecute: bool
        :param useexecute: Use :func:`execute` method of
            :mod:`sqlalchemy.engine` inside a context manager (default
            is False), or uses the :mod:`pandas.read_sql` method.
        """

        query = sa.text(query) if useexecute else query

        if useexecute:
            with self.engine.connect() as connection:
                if fetchone:
                    values = connection.execute(query).fetchone()[0]
                else:
                    values = connection.execute(query).fetchall()
        else:
            values = pd.read_sql(query, self.engine)
            values = values.head(1) if fetchone else values

        return values


    def insert_data(
        self,
        dataframe : pd.DataFrame,
        tablename : str,
        schemaname : str
    ) -> bool:
        """
        Insert Data into the Table using SQL Alchemy Context Manager
        """

        metadata = sa.MetaData(schema = schemaname)
        dataframe = dataframe.copy().to_dict(orient = "records")

        with self.engine.connect() as connection:
            table = sa.Table(
                tablename, metadata, autoload_with = connection
            )

            connection.execute(table.insert(), dataframe)
            connection.commit()

        return True


    def __create_engine__(
        self,
        hostname : str,
        portname : str | int,
        username : str,
        password : str,
        database : str,
        sadialect : str = "postgresql+psycopg",
    ) -> sa.Engine:
        """
        Unified function that can be used by all methods to built an
        SQLAlchemy engine and test if the connection is successful. As
        per module design, the function logs the connection status to a
        file, and verbose command is added to print to console.
        """

        set_logger(
            configfile = "./config/logging.yaml",
            outfile = "./logs/main.log"
        )
        logger = logging.getLogger("DB Connection")

        logging.info(f"Executing Function to Connect to {database}")

        engine = sa.create_engine(
            f"{sadialect}://{username}:{password}" +
            f"@{hostname}:{portname}/{database}"
        )

        try:
            engine.connect()
        except Exception:
            logger.critical("Cannot connect to the Database.")
        else:
            logger.info("Connection Established.")

        return engine

# -*- encoding: utf-8 -*-

"""
A Set of Configuration Function(s) for Main Files
"""

import yaml
import logging.config

import sqlalchemy as sa

def setLogger(configfile : str) -> None:
    """
    Setup a logger with pre-built configurations that can be used and
    extended by any endpoints to capture data update, deletion and
    other maintenance logs, using the :mod:`logger` module.

    :type  configfile: str
    :param configfile: Path to config file, this function uses PyYAML
        configuration file, check documentation for more information.
    """

    config = yaml.safe_load(open(configfile, "r").read())
    logging.config.dictConfig(config)
    return


def createEngine(
        host : str,
        port : int,
        user : str,
        password : str,
        database : str,
        logger :object,
        verbose : bool = True,
        sadialect : str = "postgresql+psycopg"
    ) -> sa.Engine:
    """
    Unified function that can be used by all methods to built an
    SQLAlchemy engine and test if the connection is successful. As
    per module design, the function logs the connection status to a
    file, and verbose command is added to print to console.

    All the required values are self-explanatory (todo), other required
    (or optional) arguments are as below:

    :type  logger: object
    :param logger: A logger object that prints information to the
        logger file, check module documentation.

    :type  verbose: bool
    :param verbose: Print connection and other details to console,
        in addition to the logger output.

    :type  sadialect: str
    :param sadialect: SQLAlchemy dialects to establish connection to
        the database, check https://docs.sqlalchemy.org/en/14/dialects/.
    """

    engine = sa.create_engine(
        f"{sadialect}://{user}:{password}@{host}:{port}/{database}"
    )

    try:
        engine.connect()
    except Exception as err:
        message = f"Cannot connect to database. Error: {err}"

        if verbose:
            print(message)

        logger.critical(message)
    else:
        message = f"Connection established to {database}"

        if verbose:
            print(message)

        logger.info(message)

    return engine

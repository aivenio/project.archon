# -*- encoding: utf-8 -*-

"""
A Set of Configuration Function(s) for Main Files
"""

import yaml
import logging.config

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

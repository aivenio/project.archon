# -*- encoding: utf-8 -*-

"""
A Set of Configuration Function(s) for Main Files
"""

import logging
import logging.handlers

def setLogger(
        name : str = None,
        level : int = logging.DEBUG,
        filename : str = "./status.log"
    ) -> object:
    """
    Setup a logger with pre-built configurations that can be used and
    extended by any endpoints to capture data update, deletion and
    other maintenance logs, using the :mod:`logger` module.

    :type  name: str
    :param name: Name of the logger, else defaults to file name as the
        name (``__name__`` of the execution script).

    :type  level: int
    :param level: Any one of the allowed levels from the :mod:`logger`
        can be used, defaults to debugging mode.

    :type  filename: str
    :param filename: Full absolute/relative path of the logger file to
        store the message, defaults to ``./status.log`` file.
    """

    logger = logging.getLogger(name or __name__)
    logger.setLevel(level)
    
    filehandler = logging.handlers.RotatingFileHandler(
        filename,
        maxBytes = 1024 * 1024,
        backupCount = 1,
        encoding = "utf8"
    )

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    filehandler.setFormatter(formatter)
    logger.addHandler(filehandler)
    return logger

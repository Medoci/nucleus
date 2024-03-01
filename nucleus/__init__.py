__version__ = "0.0.1"

import logging
import os
import sys
import datetime
import warnings

MODULE_PATH = os.path.dirname(os.path.realpath(__file__))
REPO_PATH = os.path.dirname(MODULE_PATH)
PARENT_PATH = os.path.abspath(os.path.join(REPO_PATH, os.pardir))
DRIVER_NAME = 'DRIVER_NAME'
SERVER_NAME = 'SERVER_NAME'

engine = None

# Local storage path
DATA_PATH = f"{REPO_PATH}/data/"

# Conmfiguring logs
logger = logging.getLogger(__name__)
logger.propagate = False
handler = logging.StreamHandler(stream=sys.stdout)
formatter = logging.Formatter("[%(levelname)5s] %(asctime)5s %(filename)-15.15s:%(lineno)03d - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

# Get today's date
LOAD_DATE: str = datetime.datetime.now().strftime('%Y-%m-%d')


def set_engine(_engine):
    """
    Establish an engine object to connect to SQL server
    """
    global engine
    engine = _engine


def get_engine():
    """
    Get the current engine
    """
    if engine is None:
        warnings.warn("Session is None, make sure to initilise the engine with set_engine().")


def get_connection():
    """
    Get the current session

    Returns
        pypyodbc.Connection: The current connection object
    """
    if engine is None:
        warnings.warn("Session is None, make sure to initilise the engine with set_engine().")
    return engine.connect()


def disconnect_engine():
    """
    Close the current session
    """
    if engine is None:
        warnings.warn("No engine has been initilised yet!")
    engine.disconnect()


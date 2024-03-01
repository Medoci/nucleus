import pytest
import nucleus
import pandas as pd
from nucleus.utils.sql_utils import run_query

from nucleus.connectors.microsoft_sql import MicrosoftSqlConnector

"""
The testing database where we will run all our tests on to check functionality.
"""
TEST_DATABASE = 'cactus'
TEST_TABLE = 'dbo.Cacti'

@pytest.fixture(scope='module')
def setup_engine():
    """
    Sets up the connection to the SQL server
    """
    connector = MicrosoftSqlConnector()
    engine = connector.get_engine(database=TEST_DATABASE)
    nucleus.set_engine(engine)
    conn = nucleus.get_connection()
    
    yield conn

    nucleus.disconnect_engine()


def test_odbc_connection(setup_engine):
    """
    Checks whether we can successfully connect to the SQL server
    """
    assert setup_engine != None


def test_run_query(setup_engine):
    """
    Checks whether we can successfully query the contents of a table.
    """
    expected_length = 5
    df = pd.read_sql(sql=f"SELECT * FROM {TEST_TABLE}", con=setup_engine)
    # df = run_query(f"SELECT * FROM {TEST_TABLE}")

    assert expected_length == len(df)
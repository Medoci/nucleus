import logging
import os
import nucleus.connectors.engine.odbc as odbc
from nucleus import DRIVER_NAME, SERVER_NAME

log = logging.getLogger(__name__)

class MicrosoftSqlConnector:
    """
    This connector allows the user to create an engine based on provided credentials. This will allow them to create an instance to be able to query
    the MicroSoft SQL server. 
    """

    @staticmethod
    def get_env_var(key: str) -> str:
        """
        Retrieve value from environmental varaible using key

        Returns:
            str: value from environmental key
        """
        _value = os.getenv(key)
        if _value is None:
            raise Exception(f"Environment variable {key} does not exist.")

        return _value
    

    def get_engine(self, database: str = None, **kwargs):
        """
        Creates and returns a pypyodbc engine. This will allow you to connect to the SQL server and be able to query the data.
        Credentials are discovered from the saved environmental variables. This current iteration of connection handlers is not
        intended for online SQL hosting use as it would need to be more secure. The user will need to point to which database they would like to access

        Parameters:
            database (str): The database to create a connection object to.
            kwargs (dict): The provided credentials needed for a successful connection, else defaults to env var. Intended to be useful
            if the user has more than one account.

        Returns:
            Engine: engine
        """
        if database is None:
            log.error("Database hasn't been specified. Please define which database you want to access")
        # Assign values needed to establish the engine
        driver = self.get_env_var(kwargs.get('driver') if kwargs.get('driver') else DRIVER_NAME)
        server = self.get_env_var(kwargs.get('server') if kwargs.get('server') else SERVER_NAME)

        # Packs credentials
        credentials = {
            'driver': driver,
            'server': server,
            'database': database,
        }
        
        return odbc.OdbcEngine(**credentials)

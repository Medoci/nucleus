import logging
import os
import nucleus.connectors.engine.odbc as odbc
from nucleus import SQL_HOST, SQL_USERNAME, SQL_PASSWORD

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
            raise Exception(f"Enviroment variable {key} does not exist.")

        return _value
    

    def get_engine(self, **kwargs):
        """
        Creates and returns a pypyodbc engine. This will allow you to connect to the SQL server and be able to query the data.
        Credentials are discovered from the saved environmental variables. This current iteration of connection handlers is not
        intended for online SQL hosting use as it would need to be more secure.

        Parameters:
            kwargs (dict): The provided credentials needed for a successful connection, else defaults to env var. Intended to be useful
            if the user has more than one account.

        Returns:
            Engine: engine
        """
        # Assign values needed to establish the engine
        host = self.get_env_var(kwargs.get('host') if kwargs.get('host') else SQL_HOST)
        username = self.get_env_var(kwargs.get('username') if kwargs.get('username') else SQL_USERNAME)
        password = self.get_env_var(kwargs.get('password') if kwargs.get('password') else SQL_PASSWORD)

        # Packs credentials
        credentials = {
            'host': host,
            'username': username,
            'password': password,
        }
        
        return odbc.OdbcEngine(**credentials)

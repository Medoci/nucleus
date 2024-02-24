import logging

import pypyodbc as odbc

log = logging.getLogger(__name__)

class OdbcEngine:
    """
    This class is used to connect to the MicroSoft SQL database by returning the connection object. 
    """

    def __init__(self, host=None, database=None, username=None, password=None):
        """
        Initialise the engine object with host, username and password.

        Parameters:
            host (str, optional): Server name or IP address of the server.
            database (str, optional): Name of the database to access
            username (str, optional): The username for MicroSoft SQL Server
            password (str, optional): The password for MicroSoft SQL Server
        """
        self.host = host
        self.database = database
        self.username = username
        self.password = password
        self.connection = None


    def connect(self):
        """
        Establishes a connection to the Microsoft SQL Server database.
        
        Returns:
            pyodbc.Connection or None: A connection object if successful, None otherwise.
        """
        conn = None

        # Attempt to use credential to connect to the specified server and database
        try:
            conn = odbc.connect(f"DRIVER=ODBC Driver 17 for SQL Server;
                        SERVER={self.host};
                        DATABASE={self.database};
                        UID={self.username};
                        PWD={self.password};
                        Trust_Connection=yes;")
            log.info("Connection established successfully.")
            return conn
        except Exception as e:
            log.error(f"Error connecting to the database: {str(e)}")
            return None


    def disconnect(self):
        """
        Close the connection to the MicroSoft SQL Server and delete the engine object
        """
        if self.connection:
            self.connection.close()
        del self
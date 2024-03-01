import logging

import pypyodbc as odbc

log = logging.getLogger(__name__)

class OdbcEngine:
    """
    This class is used to connect to the MicroSoft SQL database by returning the connection object. 
    """

    def __init__(self, driver=None, server=None, database=None):
        """
        Initialise the engine object with host, username and password.

        Parameters:
            driver (str, optional): The driver name
            server (str, optional): The server name
            database (str, optional): Name of the database to access
        """
        self.driver = driver
        self.server = server
        self.database = database
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
            conn = odbc.connect(f"""
                        DRIVER={{{self.driver}}};
                        SERVER={{{self.server}}};
                        DATABASE={{{self.database}}};
                        Trust_Connection=yes;""")
            log.info("Connection established successfully.""")
            return conn
        except Exception as e:
            log.info(f"""
                        DRIVER={{{self.driver}}};
                        SERVER={{{self.server}}};
                        DATABASE={{{self.database}}};
                        Trust_Connection=yes;""")
            log.error(f"Error connecting to the database: {str(e)}")
            return None


    def disconnect(self):
        """
        Close the connection to the MicroSoft SQL Server and delete the engine object
        """
        if self.connection:
            self.connection.close()
        del self
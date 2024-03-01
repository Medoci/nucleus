import logging
import pandas as pd

import nucleus

log = logging.getLogger(__name__)

def run_query(sql: str):
    """
    Pass a string to query the enabled database connection. 

    Parameters:
        sql (str): The sql query

    Returns:
        pd.DataFrame: The result of the sql query
    """
    conn = nucleus.get_connection()
    df = pd.read_sql(sql, conn)
    conn.close()

    return df


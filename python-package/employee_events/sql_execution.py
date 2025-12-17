from sqlite3 import connect
from pathlib import Path
from functools import wraps
import pandas as pd

# Using pathlib, create a `db_path` variable
# that points to the absolute path for the `employee_events.db` file
db_path = Path(__file__).resolve().parent / 'employee_events.db'

# OPTION 1: MIXIN
# Define a class called `QueryMixin`


class QueryMixin:

    # Define a method named `pandas_query`
    # that receives an sql query as a string
    # and returns the query's result
    # as a pandas dataframe
    def pandas_query(self, sql_query):

        connection = connect(db_path)

        try:
            df = pd.read_sql(sql_query, connection)
            return df
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    # Define a method named `query`
    # that receives an sql_query as a string
    # and returns the query's result as
    # a list of tuples. (You will need
    # to use an sqlite3 cursor)

    def query(self, sql_query):

        # 1. Create a cursor object from the connection
        connection = connect(db_path)
        cursor = connection.cursor()

        try:
            # 2. Execute the provided SQL string
            cursor.execute(sql_query)

            # 3. Fetch all results as a list of tuples
            result = cursor.fetchall()

            return result
        except Exception as e:
            print(f"An error occurred: {e}")
            return []
        finally:
            # 4. Close the cursor to free up resources
            cursor.close()

 # Leave this code unchanged


def query(func):
    """
    Decorator that runs a standard sql execution
    and returns a list of tuples
    """

    @wraps(func)
    def run_query(*args, **kwargs):
        query_string = func(*args, **kwargs)
        connection = connect(db_path)
        cursor = connection.cursor()
        result = cursor.execute(query_string).fetchall()
        connection.close()
        return result

    return run_query

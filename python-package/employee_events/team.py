# Import the QueryBase class
from employee_events.query_base import QueryBase

# Import dependencies for sql execution
from employee_events.sql_execution import QueryMixin
from employee_events import sql_execution

# Create a subclass of QueryBase
# called  `Team`
class Team(QueryBase):

    # Set the class attribute `name`
    # to the string "team"
    name = "team"


    # Define a `names` method
    # that receives no arguments
    # This method should return
    # a list of tuples from an sql execution
    @sql_execution.query
    def names(self):
        
        # Query 5
        # Write an SQL query that selects
        # the team_name and team_id columns
        # from the team table for all teams
        # in the database

        query_string = """
            SELECT 
                team_name,
                team_id
            FROM team;
        """

    # Define a `username` method
    # that receives an ID argument
    # This method should return
    # a list of tuples from an sql execution
    @sql_execution.query
    def username(self):

        # Query 6
        # Write an SQL query
        # that selects the team_name column
        # Use f-string formatting and a WHERE filter
        # to only return the team name related to
        # the ID argument

        query_string = f"""
                SELECT 
                    team_name
                FROM team
                WHERE team.team_id = {id};
        """

    # Below is method with an SQL query
    # This SQL query generates the data needed for
    # the machine learning model.
    # Without editing the query, alter this method
    # so when it is called, a pandas dataframe
    # is returns containing the execution of
    # the sql query
    @QueryMixin.pandas_query
    def model_data(self, id):

        return f"""
            SELECT positive_events, negative_events FROM (
                    SELECT employee_id
                         , SUM(positive_events) positive_events
                         , SUM(negative_events) negative_events
                    FROM {self.name}
                    JOIN employee_events
                        USING({self.name}_id)
                    WHERE {self.name}.{self.name}_id = {id}
                    GROUP BY employee_id
                   )
                """
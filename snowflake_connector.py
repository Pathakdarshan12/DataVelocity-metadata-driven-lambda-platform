import snowflake.connector
from snowflake.connector import DictCursor
import os
from contextlib import contextmanager
from typing import Optional, Dict, Any
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class SnowflakeConnection:
    """
    A class to manage Snowflake database connections.
    """

    def __init__(
            self,
            user: Optional[str] = None,
            password: Optional[str] = None,
            account: Optional[str] = None,
            warehouse: Optional[str] = None,
            database: Optional[str] = None,
            schema: Optional[str] = None,
            role: Optional[str] = None
    ):
        """
        Initialize Snowflake connection parameters.
        Defaults to environment variables if parameters not provided.
        """
        self.user = os.getenv('SNOWFLAKE_USER')
        self.password = os.getenv('SNOWFLAKE_PASSWORD')
        self.account = os.getenv('SNOWFLAKE_ACCOUNT')
        self.warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
        self.database = os.getenv('SNOWFLAKE_DATABASE')
        self.schema = os.getenv('SNOWFLAKE_SCHEMA')
        self.role = os.getenv('SNOWFLAKE_ROLE')
        self.connection = None

    def connect(self) -> snowflake.connector.SnowflakeConnection:
        """
        Establish connection to Snowflake.
        """
        try:
            conn_params = {
                'user': self.user,
                'password': self.password,
                'account': self.account
            }

            # Add optional parameters only if they are provided
            if self.warehouse:
                conn_params['warehouse'] = self.warehouse
            if self.database:
                conn_params['database'] = self.database
            if self.schema:
                conn_params['schema'] = self.schema
            if self.role:
                conn_params['role'] = self.role

            self.connection = snowflake.connector.connect(**conn_params)
            return self.connection
        except Exception as e:
            print(f"Error connecting to Snowflake: {e}")
            raise

    def close(self):
        """Close the Snowflake connection."""
        if self.connection:
            self.connection.close()

    @contextmanager
    def get_cursor(self, dict_cursor: bool = False):
        """
        Context manager for Snowflake cursor.

        Args:
            dict_cursor: If True, returns results as dictionaries
        """
        cursor = None
        try:
            if not self.connection:
                self.connect()

            cursor = self.connection.cursor(DictCursor) if dict_cursor else self.connection.cursor()
            yield cursor
        finally:
            if cursor:
                cursor.close()

    def execute_query(self, query: str, params: Optional[tuple] = None) -> list:
        """
        Execute a SELECT query and return results.

        Args:
            query: SQL query to execute
            params: Optional query parameters

        Returns:
            List of query results
        """
        with self.get_cursor() as cursor:
            cursor.execute(query, params) if params else cursor.execute(query)
            return cursor.fetchall()

    def execute_update(self, query: str, params: Optional[tuple] = None) -> int:
        """
        Execute an INSERT/UPDATE/DELETE query.

        Args:
            query: SQL query to execute
            params: Optional query parameters

        Returns:
            Number of affected rows
        """
        with self.get_cursor() as cursor:
            cursor.execute(query, params) if params else cursor.execute(query)
            self.connection.commit()
            return cursor.rowcount


# Example usage
if __name__ == "__main__":
    # Credentials will be automatically loaded from .env file
    sf = SnowflakeConnection()

    try:
        # Connect to Snowflake
        sf.connect()

        # Execute a query
        results = sf.execute_query("SELECT CURRENT_VERSION()")
        print(f"Snowflake Version: {results}")

        # Using context manager for custom queries
        with sf.get_cursor(dict_cursor=True) as cursor:
            cursor.execute("CALL ITAS.DEV.SP_ETL_JOBS_PIPELINE(%s)", ('@scrapped_jobs/jobs_2025-12-10_12-09-21.csv',))
            result = cursor.fetchone()
            print(f"Current User: {result}")

        with sf.get_cursor() as cursor:
            # Search across all schemas in the database
            cursor.execute("USE DATABASE ITAS")
            cursor.execute("SHOW PROCEDURES LIKE 'SP_ETL_JOBS_PIPELINE'")
            procedures = cursor.fetchall()
            print(f"All procedures in ITAS database: {procedures}")

        # Add after connection
        with sf.get_cursor() as cursor:
            cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_USER()")
            context = cursor.fetchone()
            print(f"Current context: {context}")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        sf.close()
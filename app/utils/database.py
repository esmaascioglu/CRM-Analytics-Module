import oracledb
import pandas as pd
import datetime
import os
import sys
import logging

from datetime import datetime as dt

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(root_dir)

# oracledb.init_oracle_client(lib_dir="/opt/ora/instantclient_19_25")


from app.config import Config
import warnings
from sqlalchemy import create_engine, inspect, text
from app.utils.general_utils import GeneralUtils
warnings.filterwarnings("ignore")

class DatabaseManager:
    def __init__(self,SCHEMA_NAME):
        self.config = Config()
        self.user = self.config.user
        self.pw = self.config.pw
        self.connection_string = self.config.connection_string
        self.cs = self.config.cs
        self.SCHEMA_NAME = SCHEMA_NAME

    def create_engine(self):
        """
        Create and return a SQLAlchemy engine using the correct connection string.
        """
        try:
            engine = create_engine(self.cs)
            logging.info("SQLAlchemy engine created successfully.")
            return engine
        except Exception as e:
            logging.error(f"Failed to create SQLAlchemy engine: {e}")
            raise

    def create_connection(self):
        """
        Creates and returns a new database connection.
        """
        try:
            connection = oracledb.connect(user=self.user, password=self.pw, dsn=self.connection_string)
            logging.info("Database connection established.")
            return connection
        except oracledb.DatabaseError as e:
            logging.error(f"Failed to connect to the database: {e}")
            raise


    def log_to_db(self, job_type, metric_id, firm_id, status, execution_start, execution_end):
        """
        Logs job execution details to the STG_LOGS table.

        Args:
            connection (oracledb.Connection): Database connection object.
            firm_id (int): Identifier of the firm.
            status (str): Status of the job ('SUCCESS', 'FAIL', or 'PENDING').
            execution_start (datetime): Start timestamp of the job execution.
            execution_end (datetime): End timestamp of the job execution.
        """
        try:
            # Ensure status is valid
            if status not in ['SUCCESS', 'FAIL', 'PENDING']:
                raise ValueError("Invalid status. Allowed values are 'SUCCESS', 'FAIL', 'PENDING'.")

            # Prepare date fields
            ett_date = execution_start.strftime('%Y%m%d')
            last_update_date = dt.now()  # system date for the update

            # Construct insert query
            insert_query = f"""
                INSERT INTO {self.config.db_admin}.STG_LOGS (FIRM_ID, METRIC_ID, JOB_TYPE, ETT_DATE, EXECUTION_START_DATE, EXECUTION_END_DATE, STATUS, LAST_UPDATE_DATE)
                VALUES (:firm_id, :metric_id, :job_type, :ett_date, :execution_start, :execution_end, :status, :last_update_date)
            """

            connection = self.create_connection()

            # Execute insert query
            with connection.cursor() as cursor:
                cursor.execute(insert_query, {
                    'firm_id': firm_id,
                    'metric_id':metric_id,
                    'job_type':job_type,
                    'ett_date': ett_date,
                    'execution_start': execution_start,
                    'execution_end': execution_end,
                    'status': status,
                    'last_update_date': last_update_date
                })
                connection.commit()
                logging.info(f"Log entry added to {self.config.db_admin}.STG_LOGS for firm_id: {firm_id}, job_type: {job_type}, metric_id: {metric_id}, status: {status}")

        except oracledb.DatabaseError as e:
            logging.error(f"Database error occurred while logging: {e}")
            raise
        except Exception as e:
            logging.error(f"An error occurred in log_to_db function: {e}")
            raise

    def get_table_columns(self, table_name: str) -> list:
        """
        Retrieves the column names of the specified table in the database.
        """
        try:
            with self.create_connection() as connection:
                query = f"""
                    SELECT COLUMN_NAME
                    FROM ALL_TAB_COLUMNS
                    WHERE TABLE_NAME = '{table_name.upper()}' AND OWNER = '{self.SCHEMA_NAME.upper()}'
                """
                columns = pd.read_sql(query, connection)['COLUMN_NAME'].tolist()
                return columns
        except Exception as e:
            logging.error(f"Error retrieving columns for {self.SCHEMA_NAME}.{table_name}: {e}")
            raise

    def insert_data_to_db(self, df: pd.DataFrame, table_name: str, batch_size: int = 1000):
        """
        Inserts data from a DataFrame into the specified table in the database using batch processing.

        Args:
            df (pd.DataFrame): DataFrame containing the data to be inserted.
            table_name (str): Name of the table to insert data into.
            batch_size (int): Number of rows to process in each batch.
        """
        # Retrieve columns in the target table and filter DataFrame columns
        table_columns = self.get_table_columns(table_name)
        df = df[[col for col in df.columns if col in table_columns]]

        # Construct the insert query with placeholders
        insert_query = f"""
            INSERT INTO {self.SCHEMA_NAME}.{table_name} ({', '.join(df.columns)}) 
            VALUES ({', '.join([':' + str(i + 1) for i in range(len(df.columns))])})
        """

        try:
            with self.create_connection() as connection:
                with connection.cursor() as cursor:
                    total_rows = len(df)
                    for start_idx in range(0, total_rows, batch_size):
                        end_idx = min(start_idx + batch_size, total_rows)
                        batch_df = df.iloc[start_idx:end_idx]

                        # Prepare data with formatted rows
                        data = []
                        for _, row in batch_df.iterrows():
                            formatted_row = [
                                oracledb.Timestamp(value.year, value.month, value.day, value.hour, value.minute, value.second)
                                if isinstance(value, datetime.datetime) else value
                                for value in row
                            ]
                            data.append(formatted_row)

                        # Execute the batch insert
                        cursor.executemany(insert_query, data)
                        connection.commit()

                    logging.info(f"Inserted {total_rows} rows into {self.SCHEMA_NAME}.{table_name}.")

        except oracledb.DatabaseError as e:
            logging.error(f"Error inserting data into {self.SCHEMA_NAME}.{table_name}: {e}")
            raise

    def delete_all_records(self, table_name: str):
        """
        Deletes all records from the specified table.
        """
        delete_query = f"DELETE FROM {self.SCHEMA_NAME}_ELT.{table_name}"
        try:
            with self.create_connection() as connection:
                logging.error(f"Truncating table: {table_name}.")
                with connection.cursor() as cursor:
                    cursor.execute(delete_query)
                    connection.commit()
                    logging.error(f"All records has been deleted from {table_name}.")
        except oracledb.DatabaseError as e:
            logging.error(f"Error deleting records from {table_name}: {e}")
            raise

    def execute_query(self, query_path: str, dt_start:str, dt_end:str):
        """
        Executes a SQL query read from a file.
        """
        try:
            with open(query_path, "r", encoding="utf-8") as f:
                query = f.read()                

                query = GeneralUtils.format_schema_name(query, self.SCHEMA_NAME, dt_start=dt_start, dt_end=dt_end)
                query = query.strip(" \n;")

            with self.create_connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(query)
                    connection.commit()
                    logging.info(f"Query executed from file: {query_path}")
        except (oracledb.DatabaseError, FileNotFoundError) as e:
            logging.error(f"Error executing query: {e}")
            raise


    def execute_queries(self, queries: list, start_dt: str, end_dt: str, schema_name: str):
        """
        Execute a list of SQL queries on the database.
        
        Args:
            queries (list): List of file paths to SQL query files.
            start_dt (str): Start date for query placeholders.
            end_dt (str): End date for query placeholders.
            schema_name (str): Schema name to be replaced in the queries.
        """
        try:
            with self.create_connection() as connection:
                with connection.cursor() as cursor:
                    for query_file in queries:
                        with open(query_file, "r", encoding="utf-8") as f:
                            query = f.read()

                        # Replace placeholders with actual values
                        query = query.replace("{start_dt}", start_dt)
                        query = query.replace("{end_dt}", end_dt)
                        query = query.replace("{SCHEMA_NAME}", schema_name)
                        query = query.replace("{schema_name}", schema_name)
                        cursor.execute(query)
                        connection.commit()

                        logging.info(f"Executed query from {query_file}")
        except Exception as e:
            logging.error(f"Error executing queries: {e}")
            raise

    def table_checker(self, prefix: str = "opt_"):
        """
        Check for tables with a given prefix in the database using SQLAlchemy inspection.
        """
        try:
            engine = self.create_engine()
            inspector = inspect(engine)
            tables = inspector.get_table_names()
            filtered_tables = [table for table in tables if table.startswith(prefix)]
            logging.info(f"Tables with prefix '{prefix}': {filtered_tables}")
            return filtered_tables
        except Exception as e:
            logging.error(f"Error checking tables: {e}")
            raise

    def drop_tables(self, table_names: list):
        """
        Drops the tables from the database based on the list of table names.
        
        Args:
            table_names (list): A list of table names to be dropped.
        """
        if len(table_names) > 0: 
                
            try:
                engine = self.create_engine()
                with engine.connect() as connection:
                    for table in table_names:
                        try:
                            drop_query = text(f"DROP TABLE {table} PURGE")
                            connection.execute(drop_query)
                            logging.info(f"Table {table} dropped successfully.")
                        except Exception as e:
                            logging.error(f"Error dropping table {table}: {e}")
            except Exception as e:
                logging.error(f"Error while dropping tables: {e}")
                raise

        else: 
            pass
        
    def fetch_data_as_df(self, query: str, batch_size: int = 10000) -> pd.DataFrame:
        """
        Executes a SQL query and returns the result as a pandas DataFrame with optimized fetching using oracledb.
        
        Args:
            query (str): SQL query to execute.
            batch_size (int): Number of rows to fetch per batch.
            
        Returns:
            pd.DataFrame: Resulting data from the query as a DataFrame.
        """
        try:
            # Establish a connection
            with self.create_connection() as connection:
                cursor = connection.cursor()
                logging.info("Executing query...")

                cursor.execute(query)

                # Get column names
                column_names = [col[0] for col in cursor.description]

                # Fetch data in chunks
                logging.info("Fetching data in batches...")
                data = []
                while True:
                    rows = cursor.fetchmany(batch_size)
                    if not rows:
                        break
                    data.extend(rows)

                # Convert to DataFrame
                df = pd.DataFrame(data, columns=column_names)
                logging.info("Data successfully fetched and converted to DataFrame.")
                return df

        except oracledb.DatabaseError as e:
            logging.error(f"Database error: {e}")
            raise
        except Exception as e:
            logging.error(f"Error fetching data: {e}")
            raise

    def delete_all_records_in_table(self, table_name: str):
        """
        Deletes all records from the specified table in the database.
        
        Args:
            table_name (str): The name of the table from which to delete all records.
        """
        delete_query = f"TRUNCATE TABLE {self.SCHEMA_NAME}.{table_name}"
        try:
            with self.create_connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(delete_query)
                    connection.commit()
                    logging.info(f"All records deleted from {self.SCHEMA_NAME}.{table_name}.")
        except oracledb.DatabaseError as e:
            logging.error(f"Error truncating table {self.SCHEMA_NAME}.{table_name}: {e}")
            raise





    





        
import polars as pl
import logging
from sqlalchemy import create_engine


class FileManager:
    def __init__(self, config):
        self.config = config

    # def save_to_parquet(self, query):
    #     logging.error("Fetching data from DB...")
    #     df = pl.read_database_uri(query, self.config.cs)
    #     logging.error("Saving data to parquet...")
    #     df.to_pandas().to_parquet(self.config.file_name, index=False)
    #     logging.error(f"Data saved to {self.config.file_name}.")



    def save_to_parquet(self, query):
        logging.error("Fetching data from DB...")

        try:
            engine = create_engine(self.config.cs)
            # Using Polars' read_sql with SQLAlchemy engine connection
            df = pl.read_sql(query, engine)
            
            logging.error("Saving data to parquet...")
            df.to_pandas().to_parquet(self.config.file_name, index=False)
            logging.error(f"Data saved to {self.config.file_name}.")

        except Exception as e:
            logging.error(f"Failed to fetch and save data: {e}")
            raise

import time
import logging
import os, glob
import sys
from datetime import datetime

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(root_dir)

from app.config import Config 
from app.utils.database import DatabaseManager
from app.utils.file import FileManager
from app.utils.general_utils import GeneralUtils

class Data_Prep_Runner:
    def __init__(self, SCHEMA_NAME, firm_id, dt_start, dt_end):
        self.SCHEMA_NAME = SCHEMA_NAME
        self.dt_start = dt_start
        self.dt_end = dt_end
        self.config = Config()
        self.db_manager = DatabaseManager(self.SCHEMA_NAME)
        self.file_manager = FileManager(self.config)
        self.utils = GeneralUtils
        self.job_name = "RFM_CLV_data_prep_job"
        self.start_time = datetime.fromtimestamp(time.time())
        self.firm_id = firm_id

    def run(self):

        try:
            logging.error(f"Starting job: {self.job_name}")

            self.db_manager.log_to_db(job_type=self.job_name, metric_id=0, firm_id=self.firm_id, status='PENDING', execution_start=self.start_time, execution_end=datetime.fromtimestamp(time.time()))

            queries = glob.glob(os.path.join(root_dir, "db_queries", "segmentasyon", "*.sql"))

            tables_to_drop = ["RFM_STG","ANALYTICAL_PROFILE","ANALYTIC_ALL_DATA","ANALYTIC_CUSTOMER" ]


            for table in tables_to_drop:

                self.db_manager.delete_all_records(table_name=table)

            # # Execute all

            for q in queries:

                self.db_manager.execute_query(q, self.dt_start, self.dt_end)
                
            # Save locally

            all_data_query = f"SELECT * FROM {self.SCHEMA_NAME}_ELT.ANALYTIC_ALL_DATA"
            all_data = self.db_manager.fetch_data_as_df(all_data_query)

            all_data.to_parquet(f"data/{self.SCHEMA_NAME}_all_data.parquet", index=False)

            # success message            
            end_time = datetime.fromtimestamp(time.time())

            self.db_manager.log_to_db(job_type=self.job_name, metric_id=0, firm_id=self.firm_id, status='SUCCESS', execution_start=self.start_time, execution_end=end_time)

        except Exception as e:
            logging.error(f"Error during job: {e}")
            
            end_time = datetime.fromtimestamp(time.time())
            self.db_manager.log_to_db(job_type=self.job_name, metric_id=0, firm_id=self.firm_id, status='FAIL', execution_start=self.start_time, execution_end=end_time)









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
    def __init__(self, SCHEMA_NAME, firm_id, CHURN_THRESHOLD):
        self.SCHEMA_NAME = SCHEMA_NAME
        self.CHURN_THRESHOLD = str(CHURN_THRESHOLD)
        self.config = Config()
        self.db_manager = DatabaseManager(self.SCHEMA_NAME)
        self.file_manager = FileManager(self.config)
        self.utils = GeneralUtils
        self.job_name = "Churn_data_prep_job"
        self.start_time = datetime.fromtimestamp(time.time())
        self.firm_id = firm_id

    def run(self):

        try:
            logging.error(f"Starting job: {self.job_name}")

            # self.db_manager.log_to_db(job_type=self.job_name, metric_id=0, firm_id=self.firm_id, status='PENDING', execution_start=self.start_time, execution_end=datetime.fromtimestamp(time.time()))

            queries = glob.glob(os.path.join(root_dir, "db_queries", "churn", "*.sql"))

            tables_to_drop = ["ANALYTIC_CUSTOMER_BASE"]

            for table in tables_to_drop:

                self.db_manager.delete_all_records(table_name=table)
            
            for q_path in queries:
        
                if q_path.lower().endswith("v0.sql"):

                    self.db_manager.execute_queries(queries=[q_path], start_dt='', end_dt='', schema_name=self.SCHEMA_NAME)
                    query = f'SELECT * FROM {self.SCHEMA_NAME}_ELT.ANALYTIC_CUSTOMER_BASE'
                    customer_base_df = self.db_manager.fetch_data_as_df(query)

                    # # save customer base df as parquet file.
                    customer_base_df.to_parquet(f"data/churn/TR_{self.SCHEMA_NAME}_churn_custBase.parquet", index=False)

                else:
                        
                    with open(q_path, "r", encoding="utf-8") as f:

                        query = f.read()

                        query = query.replace("{SCHEMA_NAME}", self.SCHEMA_NAME)
                        query = query.replace("{CHURN_THRESHOLD}", self.CHURN_THRESHOLD)
                        
                        if q_path.lower().endswith("v1.sql"): # train dataset

                            churn_train_df = self.db_manager.fetch_data_as_df(query)

                            churn_train_df.to_parquet(f"data/churn/TR_{self.SCHEMA_NAME}_churn_dataset.parquet", index=False)

                        else: 

                            churn_prediction_df = self.db_manager.fetch_data_as_df(query)

                            churn_prediction_df.to_parquet(f"data/churn/PR_{self.SCHEMA_NAME}_churn_dataset.parquet", index=False)


        except Exception as e:

            logging.error(f"Error during job: {e}")


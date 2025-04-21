import warnings 
warnings.filterwarnings('ignore')
import logging
import time
import os, glob, sys
import json
from datetime import datetime

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(root_dir)

from config import Config 
from utils.database import DatabaseManager
from utils.file import FileManager
from utils.general_utils import GeneralUtils
from segmentation.segment import Segmentation_Runner
from segmentation.data_prep import Data_Prep_Runner
from utils.smart_insight_utils import SmartInsight
from churn.data_prep import Data_Prep_Runner as Churn_Data_Prep
from churn.modelling import Churn

import pandas as pd

class CRM:

    def __init__(self) -> None:

        self.config = Config()
        self.db_manager = DatabaseManager(self.config.db_admin)
        
    def get_firms(self):

        start = time.time()
        job_name = str(os.path.basename(__file__))
        print(job_name)

        try:
            user, pw = self.config.user, self.config.pw
            db_admin = self.config.db_admin

            logging.info("Firma Bilgileri Okunuyor")

            connection = self.db_manager.create_engine()
            con = connection.connect()
            logging.info("DB baglantisi olusturuldu.")

            firms_df = pd.read_sql(f"SELECT * FROM {db_admin}.FIRMS_STG", con)
            
            firms_df.columns = firms_df.columns.map(str.upper)
            firms_df["ID"] = firms_df["ID"].astype(int)

            firms_df = firms_df[['ID','FIRM_NAME','CONN_DATA_USER_ELT','CONN_DATA_USER_CDP']].drop_duplicates()
            
            return firms_df

        except Exception as e: 
            print("Hata: %s" % e)
            logging.error("Hata: %s." % e)
            logging.error("Teams'e hata mesaji gonderildi.")

    def run_tasks(self):

        firm_df = self.get_firms()

        for index, row in firm_df.iterrows():
            firm_id = row['ID']
            firm_name = row['FIRM_NAME']
            conn_data_user_elt = row['CONN_DATA_USER_ELT']
            conn_data_user_cdp = row['CONN_DATA_USER_CDP']

            # Log firm details
            logging.info(f'Firma Bilgileri Okunuyor.\nFIRM ID: {firm_id}\nFIRM_NAME: {firm_name}\nDATA_USER_ELT: {conn_data_user_elt}\nDATA_USER_CDP: {conn_data_user_cdp}\n')

            data_prep = Data_Prep_Runner(SCHEMA_NAME=conn_data_user_cdp, firm_id=firm_id,dt_start='',dt_end='')
            data_prep.run()

            connection = self.db_manager.create_engine()
            con = connection.connect()
            logging.info("DB baglantisi olusturuldu.")

            query = f"SELECT * FROM {conn_data_user_elt}.ANALYTIC_METRICS"
            tasks_df = pd.read_sql(query, con)
            tasks_df.columns = tasks_df.columns.map(str.upper)

            if tasks_df.empty:
                logging.error(f'{firm_name} için metrik yok.')
                continue

            for i, row_ in tasks_df.iterrows():
                execution_start = datetime.now()
                METRIC_ID = row_['METRIC_ID']
                MTRC_DISPLAY_NAME = row_['DISPLAY_NAME']
                PERIOD = row_['WORKING_DAY_PERIOD']


                if METRIC_ID == 1:
                    
                    logging.error(f'{firm_name} için METRIC_ID: {METRIC_ID}, DISPLAY_NAME: {MTRC_DISPLAY_NAME} çalıştırılıyor. PERIOD = {PERIOD} Days')

                    self.db_manager.log_to_db('RFM_CLV',METRIC_ID, firm_id, 'PENDING', execution_start, None)
                    job_runner = Segmentation_Runner(SCHEMA_NAME=conn_data_user_elt, FIRM_ID=firm_id)
                    job_runner.run()
                    execution_end = datetime.now()
                    self.db_manager.log_to_db('RFM_CLV', METRIC_ID,firm_id, 'SUCCESS', execution_start, execution_end)
                    logging.info(f"RFM_CLV task completed successfully for firm {firm_name} (ID: {firm_id})")

                if METRIC_ID == 3: 

                    logging.error(f'{firm_name} için METRIC_ID: {METRIC_ID}, DISPLAY_NAME: {MTRC_DISPLAY_NAME} çalıştırılıyor. PERIOD = {PERIOD} Days')

                    self.db_manager.log_to_db('SmartInsight', METRIC_ID, firm_id, "PENDING", execution_start, execution_start)

                    execution_start = datetime.now()

                    try: 

                        smart_insight = SmartInsight(firm_id=firm_id, firm_name=firm_name, schema_name=conn_data_user_elt)
                        
                        smart_insight.run()

                        logging.info(f"Smart Insight has been generated for {firm_name}")
                        execution_end = datetime.now()

                        # Update the log
                        self.db_manager.log_to_db('Smart Insight', METRIC_ID, firm_id, 'SUCCESS', execution_start, execution_end)
                    
                    except Exception as e:
                        execution_end = datetime.now()
                        self.db_manager.log_to_db('Smart Insight', METRIC_ID, firm_id, 'FAIL', execution_start, execution_end)
                        logging.error(f"Error generating Smart Insight for firm {firm_name} (ID: {firm_id}): {e}")

                if METRIC_ID == 4: 

                    logging.error(f'{firm_name} için METRIC_ID: {METRIC_ID}, DISPLAY_NAME: {MTRC_DISPLAY_NAME} çalıştırılıyor. PERIOD = {PERIOD} Days')
                    self.db_manager.log_to_db('Churn Data Preparation', METRIC_ID, firm_id, "PENDING", execution_start, execution_start)

                    execution_start = datetime.now()

                    try:

                        #data prep for churn

                        job_runner = Churn_Data_Prep(conn_data_user_cdp, firm_id, PERIOD)
                        job_runner.run()
                        execution_end = datetime.now()

                        self.db_manager.log_to_db('Churn Data Preparation', METRIC_ID, firm_id, "SUCCESS", execution_start, execution_end)

                        # model training & prediction
                        
                        execution_start = datetime.now()

                        job_runner = Churn(firm_id, conn_data_user_cdp)
                        
                        self.db_manager.log_to_db('Churn Model Training', METRIC_ID, firm_id, "PENDING", execution_start, execution_end)

                        job_runner.run()

                        execution_end = datetime.now()
                        
                        self.db_manager.log_to_db('Churn Model Predictions', METRIC_ID, firm_id, "SUCCESS", execution_start, execution_end)
                    
                    except Exception as e:
                        execution_end = datetime.now()
                        self.db_manager.log_to_db('Churn module is not performed due to an error: {e}', METRIC_ID, firm_id, 'FAIL', execution_start, execution_end)
                        logging.error(f"Error generating Smart Insight for firm {firm_name} (ID: {firm_id}): {e}")

if '__main__':

    CRM().run_tasks()






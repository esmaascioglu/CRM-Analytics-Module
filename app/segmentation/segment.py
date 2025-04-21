import time
import logging
import os
import sys
import pandas as pd

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(root_dir)

from app.config import Config
from app.utils.database import DatabaseManager
from app.utils.file import FileManager
from app.utils.general_utils import GeneralUtils
from app.utils.segmentation_utils import SegmentationUtils

class Segmentation_Runner:

    def __init__(self, FIRM_ID: int, SCHEMA_NAME:str) -> None:
        
        self.config = Config()
        self.SCHEMA_NAME = SCHEMA_NAME
        self.FIRM_ID = FIRM_ID

        self.utils = GeneralUtils
        self.db_manager = DatabaseManager(self.SCHEMA_NAME)
        self.segment_utils = SegmentationUtils(self.FIRM_ID)

        self.start_time = time.time()

    def run(self):

        try:

            logging.error(f"Starting job: RFM_CLV")
                
            schema_name = self.SCHEMA_NAME.split("_ELT")[0]
            data = self.utils.reduce_mem(pd.read_parquet(f"data/{schema_name}_all_data.parquet"))
            data.columns = data.columns.map(str.lower)

            # Convert date columns to datetime
            data['son_alv_tarih'] = data['son_alv_tarih'].apply(self.utils.convert_to_datetime)
            data['ilk_odeme_tarih'] = data['ilk_odeme_tarih'].apply(self.utils.convert_to_datetime)

                # Remove rows with NaT in date fields
            data = data.dropna(subset=['son_alv_tarih', 'ilk_odeme_tarih'])

            out_data = pd.DataFrame()

            data = self.segment_utils.RFM_segmentation(data)                
            data = self.segment_utils.CLV_segmentation(data)

            out_data = self.segment_utils.prep_output(data)
                    
            table_name = f"ANALYTIC_CUSTOMER"
            self.db_manager.insert_data_to_db(out_data, table_name)
            logging.error(f"OUT DATAFRAME for CLV and RFM HAS BEEN INSERTED TO {self.SCHEMA_NAME}.{table_name}")

        except Exception as e:

            logging.error(f"Error during job: {e}")

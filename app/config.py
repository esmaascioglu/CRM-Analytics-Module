from pathlib import Path
from pydantic_settings import BaseSettings
from datetime import datetime
from dotenv import load_dotenv
import os 

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

# Global seed
seed = 2024

class Config(BaseSettings):

    # Database settings
    user: str = os.environ["USER"] 
    pw: str = os.environ["PW"] 

    db_admin: str = os.environ['DB_ADMIN']
    # For SQLAlchemy connection with Oracle
    connection_string: str = os.environ["CONNECTION_STRING"] 

    # SQLAlchemy Oracle connection string
    cs: str = os.environ["CS"] 

    # File paths
    file_name: str = os.environ["FILENAME"] 
    log_path: str = os.environ["LOG_PATH"] 

    # Date handling
    start_dt: str = '20240101'
    end_dt: str = datetime.now().strftime('%Y%m%d')


class ChurnConfig(BaseSettings):

    """Constants for churn model"""

    model_features: list = [

    # Recency / Duration
    'CUSTOMER_LIFETIME',
    
    # Frequency
    'DISTINCT_TRANSACTIONS', 'AVG_DAYS_BETWEEN_TRANSACTIONS',
    
    # Monetary / Value
    'AVG_SPENT', 'MAX_SPENT', 'MIN_SPENT',
    'MEDIAN_BASKET_SIZE', 'BASKET_SIZE_STDDEV',
    
    # Engagement / Promotion
    'DISCOUNTED_TRANSACTIONS',
    'TOTAL_USED_POINT', 'TOTAL_EARNED_POINT',
    
    # Segmentation / Identifiers (categorical)
    'DWH_PROGRAM_ID',
    
    # Target
    'IS_CHURN'
    ]

    cols_to_round: list = ['TOTAL_TRANSACTIONS', 'TOTAL_SPENT', 'AVG_SPENT', 'MAX_SPENT', 'MIN_SPENT',
                'DAYS_SINCE_LAST_TRANSACTION', 'CUSTOMER_LIFETIME','DISTINCT_TRANSACTIONS', 'AVG_DAYS_BETWEEN_TRANSACTIONS',
                'MEDIAN_BASKET_SIZE','BASKET_SIZE_STDDEV', 'TOTAL_DISCOUNT_EARNED', 'DISCOUNTED_TRANSACTIONS', 'TOTAL_USED_POINT',
                'TOTAL_EARNED_POINT', 'POINT_USED_TRANSACTIONS']

    categorical_features: list = ['DWH_PROGRAM_ID']

    lgbm_params : dict = {
    'objective': 'binary',
    'metric': 'auc',
    'boosting': 'gbdt',
    'learning_rate': 0.05,
    'verbose': -1,
    'seed': 42,
    'class_weight':'balanced',
    'lambda_l1': 0.1,
    'lambda_l2': 0.1,
    'num_leaves': 31,
    'max_depth': -1,
    'feature_fraction': 0.9,
    'bagging_fraction': 0.8,
    'bagging_freq': 5
    }






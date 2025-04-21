import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os
import logging
from sklearn.preprocessing import (
    StandardScaler,
    RobustScaler,
    MinMaxScaler,
    PowerTransformer,
    QuantileTransformer,
)

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(root_dir)

from app.utils.general_utils import Analytical_Utils, GeneralUtils
from app.config import Config


class SegmentationUtils:

    def __init__(self, FIRM_ID:int) -> None:
        
        self.utils = GeneralUtils
        self.a_utils = Analytical_Utils
        self.config = Config()

        self.FIRM_ID = FIRM_ID

    def RFM_segmentation(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Performs RFM segmentation and labels customers based on recency, frequency, and monetary values.
        
        Args:
            data_origin (pd.DataFrame): The original DataFrame containing customer data.

        Returns:
            pd.DataFrame: DataFrame with segmentation labels.
        """

        # Recency labelling
        today = datetime.today()

        data['recency_segment'] = 'pasif müşteri'  # Default label

        # Aktif müşteri
        data.loc[
            (data['son_alv_tarih'] >= today - timedelta(days=90)),
            'recency_segment'
        ] = 'aktif müşteri'

        # Aktif - Riskli müşteri
        data.loc[
            (data['son_alv_tarih'] >= today - timedelta(days=180)) & 
            (data['son_alv_tarih'] < today - timedelta(days=90)),
            'recency_segment'
        ] = 'aktif - riskli'

        # Yeni müşteri
        data.loc[
            (data['ilk_odeme_tarih'] >= today - timedelta(days=30)),
            'recency_segment'
        ] = 'markayla yeni temas eden' 

        data = self.a_utils.suppress_outliers(data,['monetary','frequency'])

        scaler = MinMaxScaler()

        data = self.a_utils.scale_columns(data, ['monetary','frequency'], scaler)

        # Monetary labelling
        data['monetary_segment'] = pd.cut(data['monetary_scaled'], bins=5, labels=['düşük', 'mütevazi', 'orta halli', 'yüksek', 'çok yüksek'])

        # Frequency labelling
        data.loc[data.frequency == 1, 'frequency_segment'] = 'tek alışveriş'
        data.loc[data.frequency > 1, 'frequency_segment'] = pd.cut(
            data.loc[data.frequency > 1, 'frequency_scaled'],
            bins=3,
            labels=['seyrek', 'orta', 'sık']
        )

        # Discount sensitivity labelling
        median_ind_alv_orani = data['ind_alv_orani'].median()
        data['indirim_duyarli_segment'] = 'indirime_duyarsiz'
        data.loc[data['ind_alv_orani'] > median_ind_alv_orani, 'indirim_duyarli_segment'] = 'indirime_duyarli'

        # Discount expectation labelling
        median_ort_indirim_orani = data['ort_indirim_orani'].median()
        data['indirim_beklentisi_segment'] = 'standart seviyede'
        data.loc[data['ort_indirim_orani'] > median_ort_indirim_orani, 'indirim_beklentisi_segment'] = 'yüksek seviyede'

        return data

    def CLV_segmentation(self, data:pd.DataFrame) -> pd.DataFrame:

        # Customer Lifespan (days)
        data['customer_lifespan'] = (data['son_alv_tarih'] - data['ilk_odeme_tarih']).dt.days
        data['customer_lifespan'] = data['customer_lifespan'].apply(lambda x: max(x, 1))  # Avoid division by zero
        
        # Customer Value
        data['avg_purchase_value'] = data['musteri_toplam_ciro'] / data['alisveris_adedi']
        data['avg_purchase_frequency_rate'] = data['alisveris_adedi'] / 1  # Assuming each row is one unique customer

        # CLV
        data['clv'] = round(data['avg_purchase_value'] * data['avg_purchase_frequency_rate'] * (data['customer_lifespan'] / 365), 0)
            
        clv_vip_threshold = data['clv'].quantile(0.80)
        purchase_value_vip_threshold = data['avg_purchase_value'].quantile(0.80)

        clv_loyal_threshold = data['clv'].quantile(0.50)
        frequency_loyal_threshold = data['avg_purchase_frequency_rate'].quantile(0.50)

        clv_growth_threshold = data['clv'].quantile(0.50)  
        lifespan_growth_threshold = data['customer_lifespan'].quantile(0.50)

        clv_low_threshold = data['clv'].quantile(0.30)
        # clv_risk_threshold = clv_low_threshold
        # lifespan_risk_threshold = data['customer_lifespan'].quantile(0.30)

        # segmentasyon Kuralları
        data['clv_segment'] = data.apply(

            lambda row: 'Tek Seferlik Müşteri' if row['avg_purchase_frequency_rate'] == 1 else
                        ('VIP Müşteri' if row['clv'] > clv_vip_threshold and row['avg_purchase_value'] > purchase_value_vip_threshold else
                        ('Sadık Müşteri' if row['clv'] > clv_loyal_threshold and row['avg_purchase_frequency_rate'] > frequency_loyal_threshold else
                        ('Potansiyel Büyüme Müşterisi' if row['clv'] >= clv_growth_threshold and row['customer_lifespan']>=lifespan_growth_threshold else 'Riskli Müşteri'))),
            axis=1
        )

        return data

    def prep_output(self, data:pd.DataFrame) -> pd.DataFrame:
        
                
        import oracledb
        # oracledb.init_oracle_client(lib_dir="/opt/ora/instantclient_19_25")

        CREATED_BY = self.config.user
        UPDATED_BY = self.config.user

        today = datetime.now()

        CREATE_DATE = oracledb.Date(today.year, today.month, today.day)
        UPDATE_DATE = CREATE_DATE


        ## define metrics & definitions
        metric_cols = ["ILK_ODEME_TARIH", "SON_ODEME_TARIH", "RECENCY", "FREQUENCY", "MONETARY", "RECENCY_SEGMENT", "MONETARY_SEGMENT", "FREQUENCY_SEGMENT", "INDIRIM_DUYARLI_SEGMENT", "INDIRIM_BEKLENTISI_SEGMENT", "CUSTOMER_LIFESPAN", "AVG_PURCHASE_VALUE", "CLV", "CLV_SEGMENT"]
        definition_data = {
        "METRIC_NAME": metric_cols,
        "P_CODE": ["P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8", "P9", "P10", "P11", "P12", "P14", "P15"]
        }
    
        definition_df = pd.DataFrame(definition_data)

        data.columns = data.columns.str.upper()

        data['ILK_ODEME_TARIH'] = data['ILK_ODEME_TARIH'].dt.strftime('%Y%m%d')

        #prep output for db

        cols =  ["UNIQUE_CUSTOMER_ID"] + metric_cols

        # filter metrics 
        definition_df = definition_df[definition_df['METRIC_NAME'].isin(metric_cols)]

        # only metric columns
        data = data[cols]

        melted_df = data.melt(id_vars=["UNIQUE_CUSTOMER_ID"], value_vars=definition_df["METRIC_NAME"].tolist(), var_name="METRIC_NAME", value_name="VALUE")
        merged_df = melted_df.merge(definition_df, on="METRIC_NAME")

        pivot_df = merged_df.pivot_table(index=["UNIQUE_CUSTOMER_ID"], columns="P_CODE", values="VALUE", aggfunc='first').reset_index()

        pivot_df['CREATE_DATE'] = CREATE_DATE
        pivot_df['UPDATE_DATE'] = UPDATE_DATE
        pivot_df['FIRM_ID'] = self.FIRM_ID
        pivot_df['CREATED_BY'] = CREATED_BY
        pivot_df['UPDATED_BY'] = UPDATED_BY
        pivot_df['CREATE_DATE'] = pd.to_datetime(pivot_df.CREATE_DATE)
        pivot_df['UPDATE_DATE'] = pd.to_datetime(pivot_df.UPDATE_DATE)

        pivot_df['UNIQUE_CUSTOMER_ID'] = pivot_df['UNIQUE_CUSTOMER_ID'].astype('str')

        p_columns = [col for col in pivot_df.columns if col.startswith('P')]
        pivot_df[p_columns] = pivot_df[p_columns].astype(str)

        # pivot_df = pivot_df.drop(columns=['P_CODE'])
            
        return pivot_df   


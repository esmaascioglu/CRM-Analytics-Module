import pandas as pd
import logging
from datetime import datetime
import os 
import sys
import time
import numpy as np

## for modelling
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
import joblib
from sklearn.metrics import (roc_auc_score, accuracy_score, precision_score, 
                             recall_score, f1_score, confusion_matrix, roc_curve)

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(root_dir)

from app.utils.general_utils import GeneralUtils, Analytical_Utils
from app.utils.database import DatabaseManager
from app.config import Config, ChurnConfig


class Churn:

    def __init__(self, firm_id: int, schema_name: str) -> None:
        self.firm_id = firm_id
        self.schema_name = schema_name
        self.utils = GeneralUtils
        self.analytic_utils = Analytical_Utils
        self.config = Config()
        self.parameters = ChurnConfig()

        self.MODEL_ID = GeneralUtils.generate_random_id()


        self.db_manager = DatabaseManager(self.schema_name)
        self.start_time = time.time()        

    def train_data_prep(self):
        # read data
        data = self.utils.reduce_mem(pd.read_parquet(f"data/churn/TR_{self.schema_name}_churn_dataset.parquet"))

        ## detect and supress cols with extreme values (outliers)
        df_list = []
        for program in data.DWH_PROGRAM_ID.unique():
            for i in range(2):
                df = data[(data.DWH_PROGRAM_ID == program) & (data.IS_CHURN.astype('int') == i)]
                supress_cols = self.analytic_utils.detect_extreme_outlier_columns(df)
                id_cols = ['UNIQUE_CUSTOMER_ID', 'DWH_PROGRAM_ID', 'RND']
                supress_cols = [col for col in supress_cols if col not in id_cols]
                
                df = Analytical_Utils.suppress_outliers(df, supress_cols)
                for col in supress_cols:
                    df[col] = df[f"{col}_sqrt"]
                df = df.drop(columns=[f"{col}_sqrt" for col in supress_cols])
                df_list.append(df)

        supressed_data = pd.concat(df_list)
        val_counts = supressed_data.IS_CHURN.value_counts()
        print(f"\n{self.schema_name}: DISTRIBUTION OF TARGET VARIABLE IN THE TRAIN DATASET\n{val_counts}")

        selected_features = self.parameters.model_features
        features_to_round = self.parameters.cols_to_round

        supressed_data[features_to_round] = supressed_data[features_to_round].fillna(0).round(0).astype('int')
        train_data = supressed_data[selected_features].copy()
        train_data['DWH_PROGRAM_ID'] = train_data['DWH_PROGRAM_ID'].astype('category')

        X = train_data.drop('IS_CHURN', axis=1)
        y = train_data['IS_CHURN']

        X_train, X_val, y_train, y_val = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        return X_train, X_val, y_train, y_val

    def predict_data_prep(self):
        # read data
        data = self.utils.reduce_mem(pd.read_parquet(f"data/churn/PR_{self.schema_name}_churn_dataset.parquet"))
        
        features_to_round = self.parameters.cols_to_round
        data[features_to_round] = data[features_to_round].fillna(0).round(0).astype('int')

        selected_features = self.parameters.model_features
        predict_data = data[selected_features].copy()
        predict_data['DWH_PROGRAM_ID'] = predict_data['DWH_PROGRAM_ID'].astype('category')

        customer_info = data[['UNIQUE_CUSTOMER_ID', 'DWH_PROGRAM_ID',
                              'AVG_DAYS_BETWEEN_TRANSACTIONS', 'DAYS_SINCE_LAST_TRANSACTION',
                              "CUSTOMER_LIFETIME", "DISTINCT_TRANSACTIONS", "AVG_SPENT",
                              "MAX_SPENT", "TOTAL_USED_POINT"]]
        
        X = predict_data.drop('IS_CHURN', axis=1)

        return X, customer_info

    def create_performance_metrics_df(self, auc, accuracy, precision, recall, f1, cm):
        # Flatten the confusion matrix into individual values (TN, FP, FN, TP)
        tn, fp, fn, tp = cm.ravel()
        # Log date for the metrics
        log_date = datetime.now()

        perf_data = {
            'CREATED_BY': [self.config.db_admin],
            'CREATE_DATE': [log_date],
            'FIRM_ID': [self.firm_id],
            'AUC': [auc],
            'ACCURACY': [accuracy],
            'PRECISION': [precision],
            'RECALL': [recall],
            'F1_SCORE': [f1],
            'TN': [tn],
            'FP': [fp],
            'FN': [fn],
            'TP': [tp]
        }
        performance_df = pd.DataFrame(perf_data)
        performance_df["CHRN_PRF_METRICS_ID"] = self.MODEL_ID

        return performance_df

    def train_model(self, X_train, X_val, y_train, y_val):
        categorical_features = self.parameters.categorical_features
        model_params = self.parameters.lgbm_params
    
        train_data = lgb.Dataset(
            X_train, label=y_train,
            categorical_feature=categorical_features,
            free_raw_data=False
        )
        val_data = lgb.Dataset(
            X_val, label=y_val,
            categorical_feature=categorical_features,
            reference=train_data,
            free_raw_data=False
        )
        callbacks = [
            lgb.early_stopping(stopping_rounds=50, verbose=True),
            lgb.log_evaluation(period=50)
        ]

        model = lgb.train(
            model_params,
            train_data,
            num_boost_round=1000,
            valid_sets=[train_data, val_data],
            callbacks=callbacks
        )

        feature_importance_df = self.create_feature_importance_df(model)

        y_pred_prob = model.predict(X_val, num_iteration=model.best_iteration)
        y_pred_class = (y_pred_prob >= 0.5).astype(int)

        auc = roc_auc_score(y_val, y_pred_prob)
        accuracy = accuracy_score(y_val, y_pred_class)
        precision = precision_score(y_val, y_pred_class)
        recall = recall_score(y_val, y_pred_class)
        f1 = f1_score(y_val, y_pred_class)
        cm = confusion_matrix(y_val, y_pred_class)

        print(f"Validation AUC: {auc:.4f}")
        print(f"Accuracy: {accuracy:.4f}")
        print(f"Precision: {precision:.4f}")
        print(f"Recall: {recall:.4f}")
        print(f"F1-score: {f1:.4f}")
        print("Confusion Matrix:")
        print(cm)

        # Create performance metrics dataframe with log date and other metrics
        performance_df = self.create_performance_metrics_df(auc, accuracy, precision, recall, f1, cm)

        current_dir = os.path.dirname(os.path.abspath(__file__))
        model_dir = os.path.join(current_dir, "models")
        os.makedirs(model_dir, exist_ok=True)
        model_path = os.path.join(model_dir, f"{self.schema_name}_lgbm_churn.pkl")
        self.model_path = model_path

        joblib.dump(model, model_path)

        print(f"Model training has been completed for {self.schema_name}")

        return performance_df, feature_importance_df

    def predict(self, data, customer_info):
        model = joblib.load(self.model_path)
        pred_probs = model.predict(data, num_iteration=model.best_iteration)
        pred_class = (pred_probs >= 0.6).astype(int).round(2)
        
        customer_info['churn_prob'] = pred_probs
        customer_info['IS_CHURN'] = pred_class

        return customer_info

    def postprocessing(self, data):
        conditions = [
            (data['churn_prob'] < 0.6),
            (data['churn_prob'] >= 0.6) & (data['churn_prob'] < 0.95),
            (data['churn_prob'] >= 0.95)
        ]
        choices = ['low', 'medium', 'high']
    
        data['CHURN_CLASS'] = np.select(conditions, choices, default='unknown')
        
        data['MODEL_ID'] = self.MODEL_ID

        return data

    def summarize_results(self, data):

        import oracledb
        agg_dict = {
            "UNIQUE_CUSTOMER_ID": 'count',
            "AVG_DAYS_BETWEEN_TRANSACTIONS": np.median,
            "DAYS_SINCE_LAST_TRANSACTION": np.median,
            "CUSTOMER_LIFETIME": np.median,
            "DISTINCT_TRANSACTIONS": np.median,
            "AVG_SPENT": np.median,
            "MAX_SPENT": np.median,
            "TOTAL_USED_POINT": np.median
        }
        summarized_df = data.groupby(by=['IS_CHURN', 'CHURN_CLASS', 'DWH_PROGRAM_ID']).agg(agg_dict).reset_index()
        summarized_df = summarized_df.rename(columns= {'UNIQUE_CUSTOMER_ID':'CUSTOMER_COUNT'})

        CREATED_BY = self.config.user
        UPDATED_BY = self.config.user

        today = datetime.now()

        CREATE_DATE = oracledb.Date(today.year, today.month, today.day)

        df = summarized_df.copy()

        df['CREATED_BY'] = CREATED_BY
        df['UPDATED_BY'] = UPDATED_BY
        df['CREATE_DATE'] = CREATE_DATE
        df['CHRN_FRM_SUM_ID'] = self.MODEL_ID
        df['FIRM_ID'] = self.firm_id
        
        return df

    def create_feature_importance_df(self, model):
        """
        LightGBM modelinin feature importance değerlerini kullanarak aşağıdaki alanları içeren bir df:
        
        CREATED_BY, UPDATED_BY, CREATE_DATE, UPDATE_DATE, CHRN_FEATURE_IMP_ID, FIRM_ID, FEATURE, IMPORTANCE
        
        Args:
            model: churn lighgbm model            
        Returns:
            pd.DataFrame: modelin feature importance değerlerinin yer aldığı DataFrame.
        """

        import oracledb
        # oracledb.init_oracle_client(lib_dir="/opt/ora/instantclient_19_25")

        CREATED_BY = self.config.user
        UPDATED_BY = self.config.user

        today = datetime.now()

        CREATE_DATE = oracledb.Date(today.year, today.month, today.day)
        UPDATE_DATE = CREATE_DATE
        
        feature_names = model.feature_name()
        feature_importances = model.feature_importance()
        
        records = []
        for feat, imp in zip(feature_names, feature_importances):
            record = {
                "CREATED_BY": CREATED_BY,
                "UPDATED_BY": UPDATED_BY,
                "CREATE_DATE": CREATE_DATE,
                "UPDATE_DATE": UPDATE_DATE,
                "CHRN_FEATURE_IMP_ID": self.MODEL_ID,
                "FIRM_ID": self.firm_id,
                "FEATURE": feat,
                "IMPORTANCE": imp
            }
            records.append(record)
        
        feature_imp_df = pd.DataFrame(records)
        return feature_imp_df


    def prep_output(self, data:pd.DataFrame) -> pd.DataFrame:
        
        import oracledb
        # oracledb.init_oracle_client(lib_dir="/opt/ora/instantclient_19_25")

        CREATED_BY = self.config.user
        UPDATED_BY = self.config.user

        today = datetime.now()

        CREATE_DATE = oracledb.Date(today.year, today.month, today.day)
        UPDATE_DATE = CREATE_DATE

        ## define metrics & definitions
        metric_cols = ["AVG_DAYS_BETWEEN_TRANSACTIONS", "DAYS_SINCE_LAST_TRANSACTION", "CUSTOMER_LIFETIME", "DISTINCT_TRANSACTIONS", "AVG_SPENT", "MAX_SPENT", "TOTAL_USED_POINT", "churn_prob", "IS_CHURN", "CHURN_CLASS","DWH_PROGRAM_ID", "MODEL_ID"]
        definition_data = {
        "METRIC_NAME": metric_cols,
        "P_CODE": ["P13", "P16", "P17", "P18", "P19","P20", "P21", "P22", "P23", "P24","P25", "P26"]
        }
    
        definition_df = pd.DataFrame(definition_data)

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
        pivot_df['FIRM_ID'] = self.firm_id
        pivot_df['CREATED_BY'] = CREATED_BY
        pivot_df['UPDATED_BY'] = UPDATED_BY
        pivot_df['CREATE_DATE'] = pd.to_datetime(pivot_df.CREATE_DATE)
        pivot_df['UPDATE_DATE'] = pd.to_datetime(pivot_df.UPDATE_DATE)
        pivot_df['UNIQUE_CUSTOMER_ID'] = pivot_df['UNIQUE_CUSTOMER_ID'].astype('str')

        p_columns = [col for col in pivot_df.columns if col.startswith('P')]
        pivot_df[p_columns] = pivot_df[p_columns].astype(str)
            
        return pivot_df   


    def run(self):
        # train-data prep
        X_train, X_val, y_train, y_val = self.train_data_prep()

        # predict-data prep
        X, customer_info = self.predict_data_prep()

        # train churn prediction model and generate performance metrics
        performance_metrics, feature_importances = self.train_model(X_train, X_val, y_train, y_val)
        table_name = f"CHURN_PERFORMANCE_METRICS"

        db_manager = DatabaseManager(f"{self.schema_name}_ELT")
        db_manager.insert_data_to_db(performance_metrics, table_name)

        logging.error(f"CHURN PERFORMANCE METRICS DATAFRAME for CHURN HAS BEEN INSERTED TO {self.schema_name}.{table_name}")

        table_name = "CHURN_FEATURE_IMPORTANCES"
        db_manager.insert_data_to_db(feature_importances, table_name)

        logging.error(f"CHURN MODEL FEATURE IMPORTANCES DATAFRAME for CHURN HAS BEEN INSERTED TO {self.schema_name}.{table_name}")

        # predict
        predictions = self.predict(X, customer_info)

        result = self.postprocessing(predictions)

        result['IS_CHURN'] = result['IS_CHURN'].apply(lambda x: 'CHURN' if x == 1 else 'CHURN RİSKİ YOK')

        out_data = self.prep_output(result)

        table_name = f"ANALYTIC_CUSTOMER"
        db_manager.insert_data_to_db(out_data, table_name)
        logging.error(f"CHURN CUSTOMER RESULT DATAFRAME for CLV and RFM HAS BEEN INSERTED TO {self.schema_name}.{table_name}")

        print(f"\nCHURN PIPELINE HAS BEEN COMPLETED FOR {self.schema_name}.")

        result_sum = self.summarize_results(result)

        table_name = f"CHURN_FIRM_BASED"
        db_manager.insert_data_to_db(result_sum, table_name)
        logging.error(f"CHURN RESULT SUMMARY DATAFRAME for CLV and RFM HAS BEEN INSERTED TO {self.schema_name}.{table_name}")

        logging.error(f"OUT DATAFRAME for CHURN HAS BEEN INSERTED TO {self.schema_name}.{table_name}")

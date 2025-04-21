from datetime import datetime
import pandas as pd
import numpy as np
import uuid

class GeneralUtils:
 
    @staticmethod
    def format_schema_name(query: str, SCHEMA_NAME: str, dt_start:str, dt_end:str):
        """
        Reads an SQL file and replaces schema name, dt_start and dt_end placeholders.
        """
        # with open(query_path, "r", encoding="UTF-8") as f:
        query = query.replace("{SCHEMA_NAME}", SCHEMA_NAME)
        query = query.replace("{dt_start}",dt_start).replace("{dt_end}",dt_end)
        
        return query

    def generate_random_id():
        import datetime
        now = datetime.datetime.now()
        unique_id = int(now.strftime("%Y%m%d%H%M%S%f"))
        return unique_id

    @staticmethod
    def reduce_mem(df: pd.DataFrame) -> pd.DataFrame:
        """
        Reduces memory usage of a DataFrame by downcasting numerical columns.
        """
        for c in df.select_dtypes("int64").columns:
            df[c] = df[c].astype(np.int32)

        for c in df.select_dtypes("float64").columns:
            df[c] = df[c].astype(np.float32)

        binary_cols = df.nunique().to_frame("unique_count").query("unique_count == 2").index.tolist()
        for c in df[binary_cols].select_dtypes("int32").columns:
            df[c] = df[c].astype(np.int8)

        return df

    @staticmethod
    def convert_to_datetime(date_num):
        """Converts a numeric date in 'YYYYMMDD' format to a datetime object.

        Args:
            date_num: The numeric date value.

        Returns:
            A datetime object if the conversion is successful, otherwise pd.NaT.
        """
        try:
            date_str = str(date_num).zfill(8)  # Ensure 8 digits by adding leading zeros
            return datetime.strptime(date_str, '%Y%m%d')
        except ValueError:
            return pd.NaT


    @staticmethod
    def sql_dtype_setter(df: pd.DataFrame) -> dict:

        from sqlalchemy import types

        obj = df.select_dtypes(include=[object]).columns.tolist()
        flt = df.select_dtypes(include=[float]).columns.tolist()
        ints = df.select_dtypes(include=[int]).columns.tolist()
        sch_int = {c: types.Integer for c in ints}
        sch_flt = {c: types.Float for c in flt}
        sch_obj = {c: types.VARCHAR(df[c].str.len().max()) for c in obj}

        dtypes = {}
        dtypes.update(sch_int)
        dtypes.update(sch_flt)
        dtypes.update(sch_obj)

        return dtypes


    @staticmethod
    def int_to_month(num:int):

        num_dict = {1:"Ocak",
                    2:"Şubat",
                    3:"Mart",
                    4:"Nisan",
                    5:"Mayıs",
                    6:"Haziran",
                    7:"Temmuz",
                    8:"Ağustos",
                    9:"Eylül",
                    10:"Ekim",
                    11:"Kasım",
                    12:"Aralık"}

        return num_dict[num]

    @staticmethod
    def tr_ek(num:int):
        
        try:

            num_dict = {
                        1:"'i",
                        2:"'si",
                        3:"'ü",
                        4:"'ü",
                        5:"'i",
                        6:"'sı",
                        7:"'si",
                        8:"'i",
                        9:"'9",
                        10:"'u",
                        11:"'i",
                        12:"'si"}    

            return num_dict[int(str(num)[-1])]
        
        except KeyError as e:

            if (num>=10 and num<100):

                zero_dict = {10:"'u",
                            20:"'si",
                            30:"'u",
                            40:"'ı",
                            50:"'si",
                            60:"'ı",
                            70:"'i",
                            80:"'i",
                            90:"'ı"}
                
                return zero_dict[num]
            
            else:

                return 'ü'


class Analytical_Utils:

    @staticmethod
    def suppress_outliers(data: pd.DataFrame, columns: list) -> pd.DataFrame:
        """
        Applies square root transformation, outlier suppression (mean + 3*std),
        and MinMax scaling on the specified columns of the DataFrame.

        Args:
            data (pd.DataFrame): Input DataFrame.
            columns (list): List of column names to apply the transformations on.

        Returns:
            pd.DataFrame: DataFrame with transformed and scaled columns.
        """

        for col in columns:
            # Apply square root transformation
            transformed_col = f"{col}_sqrt"
            data[transformed_col] = np.sqrt(data[col])

            # Calculate mean and standard deviation
            col_mean = data[transformed_col].mean()
            col_std = data[transformed_col].std()

            # Calculate outlier threshold (mean + 3*std)
            col_threshold = col_mean + 3 * col_std

            # Suppress outliers
            data[transformed_col] = np.where(data[transformed_col] > col_threshold, col_threshold, data[transformed_col])

        return data

    @staticmethod
    def scale_columns(data: pd.DataFrame, columns: list, scaler) -> pd.DataFrame:
        """
        Scales the specified columns in the DataFrame using the provided scaler.

        Args:
            data (pd.DataFrame): The input DataFrame.
            columns (list): List of column names to be scaled.
            scaler (object): Scaler object (e.g., MinMaxScaler(), StandardScaler(), etc.).

        Returns:
            pd.DataFrame: DataFrame with new scaled columns.
        """
        
        # Apply the same scaler to each specified column
        for col in columns:
            scaled_col_name = f"{col}_scaled"
            data[scaled_col_name] = scaler.fit_transform(data[[col]])
        
        return data


        
    @staticmethod
    def detect_extreme_outlier_columns(df: pd.DataFrame, threshold: float = 0.01) -> list:
        """
        Automatically detects numeric columns with a high proportion of extreme outliers.
        
        Extreme outliers are defined using the IQR method with a factor of 3:
        - Lower bound: Q1 - 3 * IQR
        - Upper bound: Q3 + 3 * IQR
        
        If the fraction of values outside these bounds is greater than the threshold,
        the column is flagged.
        
        Args:
            df (pd.DataFrame): Input DataFrame.
            threshold (float): Proportion threshold (e.g., 0.01 means 1% of values).
            
        Returns:
            List[str]: A list of column names that have extreme outliers.
        """
        extreme_cols = []
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            series = df[col].dropna()
            if series.empty:
                continue
            
            Q1 = series.quantile(0.25)
            Q3 = series.quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 3 * IQR
            upper_bound = Q3 + 3 * IQR
            
            # Identify extreme outliers
            extreme_outliers = series[(series < lower_bound) | (series > upper_bound)]
            ratio = len(extreme_outliers) / len(series)
            
            
            if ratio > threshold:
                extreme_cols.append(col)
                
        return extreme_cols
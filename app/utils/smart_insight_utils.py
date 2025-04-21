import pandas as pd
import logging
from datetime import datetime
import os 
import sys


pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(root_dir)

from app.utils.general_utils import GeneralUtils
from app.utils.database import DatabaseManager
from app.config import Config


class SmartInsight:
    def __init__(self, firm_id: int, firm_name:str, schema_name:str):
        """
        Args:
            firm_id (int): Firm ID for generating insights.
            db_manager: DatabaseManager instance for executing queries.
        """
        self.firm_id = firm_id
        self.firm_name = firm_name
        self.schema_name = schema_name
        self.utils = GeneralUtils
        self.config=Config()

        self.db_manager = DatabaseManager(self.schema_name)

    def data_prep(self, query_path: str) -> pd.DataFrame:
        """
        Prepares and fetches data by replacing schema names in the SQL query and executing it.
        
        Args:
            query_path (str): Path to the SQL query file.
            schema_name (str): Schema name to replace in the query.
        
        Returns:
            pd.DataFrame: DataFrame containing the query results.
        """
        try:
            # Read the SQL query
            with open(query_path, "r", encoding="utf-8") as file:
                query = file.read()

            # Replace schema placeholder with the actual schema name
            query = query.replace("{SCHEMA_NAME}", self.schema_name)

            # Fetch data using DatabaseManager
            logging.info(f"Executing query for schema: {self.schema_name}")
            metrics_df = self.db_manager.fetch_data_as_df(query)
            
            ## derive new - required metrics 
            metrics_df = metrics_df.rename(columns={'TOTAL_CHURNED_CUSTOMER_COUNT':'CHURNED_CUSTOMER_COUNT'})
            metrics_df['TOTAL_CHURN_RISK_CUSTOMER_PCT'] = round((metrics_df['TOTAL_CHURN_RISK_CUSTOMER_COUNT'] / metrics_df['LAST_YEAR_CUSTOMER_COUNT'])*100, 1)
            metrics_df['ACTUAL_CHURN_RATE'] = round((metrics_df['CHURNED_CUSTOMER_COUNT'] / metrics_df['TOTAL_CUSTOMERS'])*100,1)
            metrics_df['MEDIAN_MONETARY_VALUE'] = int(round(metrics_df['MEDIAN_MONETARY_VALUE'],0))
            metrics_df['MEDIAN_AVG_PURCHASE_VALUE'] = int(round(metrics_df['MEDIAN_AVG_PURCHASE_VALUE'],0))
            metrics_df['LOST_REVENUE_FROM_CHURN_RISK'] = int(round(metrics_df['LOST_REVENUE_FROM_CHURN_RISK'],0))
            metrics_df['NEW_CUSTOMER_CONTRIBUTION_PCT'] = round(metrics_df['NEW_CUSTOMER_CONTRIBUTION_PCT'],1)
            metrics_df['RETENTION_RATIO'] = round(metrics_df['RETENTION_RATIO'],1)
            metrics_df['NEW_CUSTOMER_CONTRIBUTION_PCT'] = round(metrics_df['NEW_CUSTOMER_CONTRIBUTION_PCT'],1)
            metrics_df['PROJECTED_REVENUE_FROM_ACTIVE_CUSTOMERS'] = int(round(metrics_df['PROJECTED_REVENUE_FROM_ACTIVE_CUSTOMERS'],0))

            if metrics_df.empty:
                logging.warning(f"No data returned for schema: {self.schema_name}")
            
            return metrics_df

        except Exception as e:
            logging.error(f"Error preparing data for schema {self.schema_name}: {e}")
            raise


    def fetch_segment_metrics(self) -> pd.DataFrame:
        """
        Fetch segment-based metrics from the database.
        
        Returns:
            pd.DataFrame: DataFrame containing segment-specific metrics for the firm.
        """
        query_path = "db_queries/smart_insight/firm-segment based metrics.sql"
        logging.info("Fetching segment metrics.")
        return self.db_manager.execute_query_from_file(query_path, firm_id=self.firm_id)

    def fetch_overall_firm_metrics(self) -> pd.DataFrame:
        """
        Fetch overall firm metrics by running the 'overall-firm.sql' query.
        
        Returns:
            pd.DataFrame: DataFrame containing the metrics.
        """
        query_path = "db_queries/smart_insight/overall-firm.sql"
        try:
            # Use data_prep to process the query and fetch data
            return self.data_prep(query_path=query_path)
        except Exception as e:
            logging.error(f"Error fetching overall metrics: {e}")
            raise

    def generate_overall_insight(self, metrics: pd.DataFrame) -> str:
        """
        Generate a comprehensive and insightful textual overview of the firm's customer portfolio.

        Args:
            metrics (pd.DataFrame): DataFrame containing overall metrics.

        Returns:
            str: Generated insight text in Turkish.
        """
        # Extract metrics

        total_customers = metrics['TOTAL_CUSTOMERS'].iloc[0]
        earliest_year = metrics['EARLIEST_TRN_YEAR'].iloc[0]
        earliest_month = metrics['EARLIEST_TRN_MONTH'].iloc[0]
        median_recency = metrics['MEDIAN_RECENCY'].iloc[0]
        median_frequency = metrics['MEDIAN_FREQUENCY'].iloc[0]
        median_monetary = metrics['MEDIAN_MONETARY_VALUE'].iloc[0]
        median_avg_purchase_value = metrics['MEDIAN_AVG_PURCHASE_VALUE'][0]
        last_year_customer_count = metrics['LAST_YEAR_CUSTOMER_COUNT'].iloc[0]
        active_customer_count = metrics['ACTIVE_CUSTOMER_COUNT'].iloc[0]
        active_customer_pct = metrics['ACTIVE_CUSTOMER_PCT'].iloc[0]
        one_time_conversion_rate = metrics['ONE_TIME_TO_ACTIVE_CONVERSION_RATE'].iloc[0]
        retention_ratio = metrics['RETENTION_RATIO'].iloc[0]
        lost_revenue = metrics['LOST_REVENUE_FROM_CHURN_RISK'].iloc[0]
        projected_revenue = metrics['PROJECTED_REVENUE_FROM_ACTIVE_CUSTOMERS'].iloc[0]
        churn_risk_count = metrics['CHURN_RISK_CUSTOMER_COUNT'].iloc[0]
        churn_risk_pct = metrics['TOTAL_CHURN_RISK_CUSTOMER_PCT'].iloc[0]
        churned_customer_count = metrics['CHURNED_CUSTOMER_COUNT'].iloc[0]
        churned_customer_pct = metrics['ACTUAL_CHURN_RATE'].iloc[0]
        vip_customer_pct = metrics['VIP_CUSTOMER_PCT'].iloc[0]
        loyal_customer_pct = metrics['LOYAL_CUSTOMER_PCT'].iloc[0]
        new_customer_pct = metrics['NEW_CUSTOMER_PCT'].iloc[0]
        one_time_customer_pct = metrics['ONE_TIME_CUSTOMER_PCT'].iloc[0]
        potential_growth_customer_pct = metrics['POTENTIAL_GROWTH_CUSTOMER_PCT'].iloc[0]

        # Introduction: Firm History and Customer Base
        insight = (
            f"Veritabanında yer alan {self.firm_name} verilerine göre, {earliest_year} yılının {self.utils.int_to_month(earliest_month)} ayından itibaren {total_customers:,} müşteri {self.firm_name} markasından alışveriş yaptı. "
            f"Son 12 aylık döneme bakıldığında ise toplamda {last_year_customer_count:,} müşterinin alışveriş yapmış ve bu müşterilerin %{active_customer_pct}{self.utils.tr_ek(active_customer_pct)} son üç aylık dönemde aktif olarak alışveriş yapmaya devam etmiştir. "
            f"RFM ve CLV analizleri sonucunda aktif müşterilerden beklenen bir yıllık toplam potansiyel gelir {int(round(projected_revenue,0)):,} TL olarak tahminlenmiştir. "
            f"Aktif müşterilerin yaklaşık %{retention_ratio}{self.utils.tr_ek(retention_ratio)} son 12 aylık müşteri tabanından gelmektedir."
        )
        # Conditional Insights: Retention and Churn Risks
        if retention_ratio > 80:
            insight += (
                f"Bu oran, müşteri sadakat stratejilerinizin oldukça güçlü olduğunu göstermektedir. "
                f"Sadık müşterilerinizle ilişkilerinizi geliştirmek için özel kampanyalar düzenleyebilirsiniz.\n\n"
            )
        elif retention_ratio < 50:
            insight += (
                f"Bu oran, müşteri sadakat stratejilerinizin güçlendirilmesi gerektiğine işaret ediyor. "
                f"Müşteri bağlılığını artırmak için düzenli iletişim ve ödül programları önerilebilir.\n\n"
            )

        # RFM Metrics: Recency, Frequency, Monetary
        insight += (
            f"Müşteri davranışlarını analiz etmek için kullanılan RFM (Recency-Frequency-Monetary) modelinin sonuçlarına göre, "
            f"bir müşterinin ortalama son alışveriş süresi {median_recency} gün, ortalama alışveriş sıklığı {median_frequency} "
            f"ve işlem başına ortalama harcama tutarı {int(round(median_monetary,0)):,} TL olarak ölçüldü. "
        )
        

        # Segment Definitions and Insights
        insight += (
            f"CLV ve RFM modellerinin sonuçları göz önüne alınarak {self.firm_name} markasının müşteri portföyü 6 farklı segmente ayrıldı:\n\n"
            f"**VIP Müşteriler**: İşlem başına harcama tutarları veya yaşam boyu değerleri (CLV) en üst %20'lik dilimde olan müşterilerdir. "
            f"Son bir yılda alışveriş yapmış müşterilerin %{vip_customer_pct}{self.utils.tr_ek(vip_customer_pct)} VIP segmentindedir. "
        )
        if vip_customer_pct < 10:
            insight += (
                f"VIP müşteri oranı düşük görünüyor. Bu segmenti genişletmek için özel fırsatlar ve kampanyalar oluşturulabilir.\n\n"
            )
        else:
            insight += (
                f"VIP müşteri oranı oldukça yüksek ve bu grup markaya en büyük gelir katkısını sağlıyor.\n\n"
            )


        insight += (
            f"**Sadık Müşteriler**: Düzenli alışveriş yapan ve alışveriş sıklığı üst %30’luk dilimde yer alan müşterilerden oluşuyor. "
            f"Toplam müşterilerinizin %{loyal_customer_pct}{self.utils.tr_ek(loyal_customer_pct)} sadık müşteri segmentinde yer alıyor.\n\n"
        )

        # One-Time Customers Insights
        insight += (
            f"**Tek Seferlik Müşteriler**: {self.firm_name} markasından bir kez alışveriş yapmış müşterilerdir. "
            f"Son bir yılda alışveriş yapan müşterilerin %{one_time_customer_pct}{self.utils.tr_ek(one_time_customer_pct)} ikinci kez alışveriş yapmamıştır. "
        )

        if one_time_conversion_rate > 50:
            insight += (
                f"Tek seferlik müşterilerinizin düzenli müşterilere dönüşme oranı %{one_time_conversion_rate} ile oldukça yüksek. "
                f"Tek seferlik müşteriler, iyi bir dönüşüm oranı ile sadakat segmentine taşınabilir."
                f"Bu trendi sürdürmek için yeni müşterilere kişiselleştirilmiş kampanyalar düzenleyebilirsiniz.\n\n"
            )
        else:
            insight += (
                f"Tek seferlik müşterilerin düzenli müşterilere dönüşme oranı %{one_time_conversion_rate} seviyesindedir. "
                f"Tek seferlik müşteriler, iyi bir dönüşüm oranı ile sadakat segmentine taşınabilir."
                f"Daha fazla dönüşüm için hoş geldiniz indirimleri ve müşteri ödül programları düzenleyebilirsiniz.\n\n"
            )

        # Churn Risk Insights
        insight += (
            f"**Kayıp Riski Taşıyan Müşteriler**: Son dönemlerde alışveriş yapmamış ve harcama düzeyleri düşük olan müşterilerden oluşuyor. "
        f"Son 12 ayda alışveriş yapan müşterilerinizin %{churn_risk_pct}{self.utils.tr_ek(churn_risk_pct)} bu segmentte yer alıyor. "
        f"Kayıp riski taşıyan müşterilere odaklanarak tahmini {int(round(lost_revenue,0)):,} TL'lik gelir kaybının önüne geçebilirsiniz."
        f"\n\n"
        )


        # Churned Customers
        insight += (
            f"**Kayıp Müşteriler**: Son 12 aydan uzun süredir alışveriş yapmamış müşterilerden oluşuyor. "
            f"Toplam {churned_customer_count:,} müşteri (%{churned_customer_pct}) bu segmentte yer alıyor. "
            f"Kayıp müşterilerin geri kazanımı için özel iletişim stratejileri geliştirebilirsiniz.\n\n"
        )


        # Potential Growth Customers
        insight += (
            f"**Potansiyel Büyüme Müşterileri**: Harcamalarını artırma potansiyeline sahip müşterilerden oluşur. "
            f"Son 12 ayda alışveriş yapmış müşterilerinizin %{potential_growth_customer_pct}{self.utils.tr_ek(potential_growth_customer_pct)} bu segmentte yer alıyor. "
        )


        if potential_growth_customer_pct > churn_risk_pct:
            insight += (
                f"Potansiyel büyüme müşterilerinin oranı kayıp riski taşıyan müşterilerden daha yüksek. Bu da {self.firm_name} için büyüme fırsatları sunuyor. "
                f"Bu müşterilere yönelik hedefli kampanyalar ile bu segmentten elde edilen geliri arttırabilirsiniz."
            )
        else:
            insight += (
                f"Potansiyel büyüme müşterilerinin oranı kayıp riski taşıyan müşterilere kıyasla düşüktür. "
                f"Kayıp riski taşıyan müşterileri bu segmente taşımak için özel teklifler sunabilirsiniz."
            )


        return insight


    def generate_segment_insight(self, metrics: pd.DataFrame) -> str:
        """
        Generate a textual insight based on segment-specific metrics.
        
        Args:
            metrics (pd.DataFrame): DataFrame containing segment metrics.
        
        Returns:
            str: Generated insight text for segments.
        """
        insights = []
        for _, row in metrics.iterrows():
            segment_name = row['segment_name']
            customer_count = row['customer_count']
            avg_monetary = row['avg_monetary']
            avg_frequency = row['avg_frequency']

            insights.append(
                f"{segment_name} segmentinde toplam {customer_count:,} müşteri bulunmakta olup, "
                f"ortalama harcama tutarı {round(avg_monetary,0):,} TL ve alışveriş sıklığı {avg_frequency:.2f} olarak belirlenmiştir."
            )
        return " ".join(insights)

    def prep_output(self, metrics: pd.DataFrame) -> pd.DataFrame:
        """
        Prepares firm-level metrics for database insertion by aligning with definitions.

        Args:
            metrics (pd.DataFrame): DataFrame containing firm-level metrics.

        Returns:
            pd.DataFrame: Transformed DataFrame aligned with database schema.
        """

        import oracledb

        try:
            # Fetch metric definitions
            metric_def_query = f"SELECT * FROM {self.schema_name}.DEF_FIRM_METRICS"
            metric_def_df = self.db_manager.fetch_data_as_df(metric_def_query)

            # Convert column names to uppercase for consistency
            metric_def_df.columns = metric_def_df.columns.str.upper()
            metrics.columns = metrics.columns.str.upper()

            # Prepare metadata
            created_by = self.config.user
            updated_by = self.config.user
            today = datetime.now()

            create_date = oracledb.Date(today.year, today.month, today.day)
            
            # Map metrics to definition codes (P1, P2, ...)
            definition_map = metric_def_df.set_index("DEFINITION_NAME")["DEFINITION_NO"].to_dict()

            # Filter only defined metrics
            metric_columns = list(definition_map.keys())
            metrics_filtered = metrics[["FIRM_ID"] + metric_columns]

            # Rename columns using definition codes
            metrics_renamed = metrics_filtered.rename(columns=definition_map)
            
            p_columns = [col for col in metrics_renamed.columns if col.startswith('P')]
            metrics_renamed[p_columns] = metrics_renamed[p_columns].astype(str)

            # Add metadata columns
            metrics_renamed["CREATED_BY"] = created_by
            metrics_renamed["UPDATED_BY"] = updated_by
            metrics_renamed["CREATE_DATE"] = create_date
            metrics_renamed["UPDATE_DATE"] = create_date

            # Rearrange columns to match the target table schema
            db_columns = ["CREATED_BY", "UPDATED_BY", "CREATE_DATE", "UPDATE_DATE", "FIRM_ID"] + list(definition_map.values())
            final_df = metrics_renamed[db_columns]

            logging.info("Firm metrics successfully prepared for database insertion.")
            return final_df

        except Exception as e:
            logging.error(f"Error preparing firm metrics: {e}")
            raise



    def generate_insight_report(self, metrics:pd.DataFrame) -> pd.DataFrame:
        """
        Generate and return the complete insight report for the firm.
        """

        smart_insight = self.generate_overall_insight(metrics)
        metrics['SMART_INSIGHT'] = smart_insight

        return metrics

    def run(self): 

        logging.error("Starting job: SmartInsight")

        smart_insight_metrics = self.fetch_overall_firm_metrics()
        smart_insight_metrics = self.generate_insight_report(smart_insight_metrics)
        
        out_df = self.prep_output(smart_insight_metrics)

        table_name= f"ANALYTIC_FIRM_BASED"
        
        self.db_manager.insert_data_to_db(out_df, table_name)
        logging.error(f"OUT DATAFRAME for SMART INSIGHT HAS BEEN INSERTED TO {self.schema_name}.{table_name}")

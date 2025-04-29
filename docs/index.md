# CRM Analytics Platform Documentation

## Table of Contents

1.  [Introduction](#introduction)
    *   [Platform Overview](#platform-overview)
    *   [Target Audience](#target-audience)
    *   [Core Value Proposition](#core-value-proposition)
2.  [System Architecture](#system-architecture)
    *   [Overall Structure](#overall-structure)
    *   [Orchestration (`main.py`)](#orchestration-mainpy)
    *   [Module Integration & Sequential Execution](#module-integration--sequential-execution)
    *   [Containerization & Deployment Model](#containerization--deployment-model)
    *   [Technical Infrastructure (Data Layer, Utilities)](#technical-infrastructure-data-layer-utilities)
3.  [Core Concepts & Shared Mechanisms](#core-concepts--shared-mechanisms)
    *   [Data Sources & General Data Preparation Flow](#data-sources--general-data-preparation-flow)
    *   [Centralized Scheduling & Parameterization](#centralized-scheduling--parameterization)
    *   [Shared Utilities](#shared-utilities)
4.  [Analytics Modules (Core Features)](#analytics-modules-core-features)
    *   [RFM (Recency, Frequency, Monetary) Segmentation Module](#rfm-recency-frequency-monetary-segmentation-module)
    *   [CLV (Customer Lifetime Value) Segmentation Module](#clv-customer-lifetime-value-segmentation-module)
    *   [Churn Prediction & Analysis Module](#churn-prediction--analysis-module)
    *   [Smart Insight Module](#smart-insight-module)
5.  [Storage & Access of Analytics Results](#storage--access-of-analytics-results)
    *   [Main Output Tables (`ANALYTIC_CUSTOMER`, `ANALYTIC_FIRM_BASED`)](#main-output-tables-analytic_customer-analytic_firm_based)
    *   [Performance & Audit Tables](#performance--audit-tables)
    *   [Intermediate Storage (`.parquet`)](#intermediate-storage-parquet)
    *   [BI (Business Intelligence) Integration](#bi-business-intelligence-integration)
6.  [Setup & Administration](#setup--administration)
    *   [Prerequisites](#prerequisites)
    *   [Installation & Deployment Steps](#installation--deployment-steps)
    *   [Automated Updates (`restart.sh` & `cron`)](#automated-updates-restartsh--cron)
    *   [Monitoring & Troubleshooting](#monitoring--troubleshooting)
    *   [Quick Start Example (Manual Trigger)](#quick-start-example-manual-trigger)
7.  [Extensibility & Customization](#extensibility--customization)
8.  [Data Security & Compliance Considerations](#data-security--compliance-considerations)
9.  [FAQ (Frequently Asked Questions)](#faq-frequently-asked-questions)
10. [Release Notes & Roadmap](#release-notes--roadmap)
11. [Database Tables & Data Sources Overview](#database-tables--data-sources-overview)

---

## 1. Introduction

### Platform Overview
The *CRM Analytics Platform* is a containerized (`Docker`) framework designed to automate customer analytics workflows, particularly valuable for organizations running loyalty programs. It provides an end-to-end solution by integrating data preparation from an Oracle SQL database, executing Python-based `feature engineering` and `Machine Learning` (ML) processes, and delivering actionable insights back to the database or for consumption by BI (Business Intelligence) tools.

The platform focuses on common, high-value CRM analytics use cases:

-   **Segmentation:** Leverages industry-standard techniques like `RFM` (Recency, Frequency, Monetary) and `CLV` (Customer Lifetime Value) to assign customers into relevant groups based on their behavior and value. This allows implementers to tailor marketing strategies and campaigns for maximum impact.
-   **Churn Prediction:** Utilizes `Machine Learning` (specifically `LightGBM`) to proactively identify customers at risk of lapsing or leaving (churn), providing the opportunity to launch targeted retention campaigns and safeguard customer value.
-   **Smart Insights:** Distills complex customer data and analytics results (from RFM, CLV, Churn) into clear, actionable, natural language summaries and recommendations, helping marketing and CRM teams target the right customers with the right actions at the right time.

The platform is architected for **scalability** (handling potentially millions of customer records), **data security** (operating within the user's environment), and **ease of deployment**. It supports automated data ingestion pipelines (via configured SQL), advanced analytics processing, and direct database integration for insight delivery. Its goal is to enable organizations to maximize the value derived from their customer data and loyalty programs with minimal ongoing technical overhead after initial setup. The system is designed to be **extensible**, allowing developers to integrate new custom analytics modules as business needs evolve.

### Target Audience
This open-source CRM Analytics Platform is a B2B-focused solution ideal for:

-   **Organizations with Loyalty Programs:** Brands and businesses seeking to unlock deeper customer insights and automate analytics workflows for their loyalty or rewards programs.
-   **B2B Service Providers:** Companies offering analytics services to clients who operate loyalty programs.
-   **Customer Success & Marketing Teams:** Teams responsible for driving customer engagement, loyalty, and retention through data-driven strategies.
-   **Data/Business Analysts:** Professionals who need rapid, scalable analytics capabilities without building everything from scratch, while retaining the ability to customize key parameters like analysis periods or `churn` thresholds.
-   **Data Engineering & IT Teams:** Teams looking for a deployable analytics solution that integrates with their existing Oracle database infrastructure, requires manageable setup, and offers robust automation of analytics processes.

### Core Value Proposition
Deploying and utilizing the CRM Analytics Platform offers the following advantages:

-   **Automated, End-to-End Analytics:** Provides centrally scheduled, multi-tenant capable (via configuration), parameter-driven execution of segmentation, churn, and insight generation with minimal manual intervention post-setup.
-   **B2B Ready Design:** Easily configurable for different businesses or clients using parameter tables (e.g., the `firms` table) and generic logic, making it suitable for multi-tenant deployments.
-   **Scalable & Robust:** Features an ETL + ML + Output flow designed to handle large volumes of customer data (millions of rows) and operate reliably.
-   **Actionable Outputs:** Delivers ready-to-use customer segments (`RFM`, `CLV`), churn risk scores and categories (`low`, `medium`, `high`), and automated portfolio-level `Smart Insights`.
-   **Flexibility & Control:** Allows customization of critical business parameters like analysis frequency and `churn` definitions (`CHURN_THRESHOLD`) directly via database configuration without code changes. ([See Centralized Scheduling & Parameterization](#centralized-scheduling--parameterization))
-   **Integration Capabilities:** Outputs results directly to standard Oracle tables, facilitating seamless integration with BI tools (like Qlik, Power BI, Tableau) and other enterprise systems. ([See Storage & Access of Analytics Results](#storage--access-of-analytics-results))
-   **Open Source & Extensible:** Provides a solid foundation that can be extended with new features, custom models, or additional analytics modules to meet specific or evolving business requirements. ([See Extensibility & Customization](#extensibility--customization))

---

## 2. System Architecture

### Overall Structure
The CRM Analytics Platform employs a modular, container-based architecture:

-   **Central Orchestrator (`main.py`):** The single entry point that manages the execution flow.
-   **Analytics Modules:** Self-contained Python modules for `RFM`, `CLV`, `Segmentation`, `Churn`, `Smart Insight`.
-   **Data Layer (User-Provided Oracle DB):** Serves as the source for input data and the destination for analytics results, logs, and configuration parameters.
-   **Utility Modules:** Shared Python libraries for common tasks (DB interaction, data processing, logging, etc.).
-   **Container & Infrastructure:** Packaged as a `Docker` image, typically run via scheduled `cron` jobs. Configuration (like DB credentials) is managed via environment variables.

### Orchestration (`main.py`)
The `main.py` script orchestrates the entire analytics pipeline:

1.  **Initialization:** Starts upon container execution (usually via `cron`), sets up logging and environment.
2.  **Firm/Tenant Processing:** Queries the `firms` configuration table in the database to identify active tenants/firms scheduled for processing based on the current date and their `WORKING_DAY_PERIOD`.
3.  **Task Execution:** For each eligible firm, `main.py` invokes the configured analytics modules in a predefined sequence.
4.  **Module Invocation:** Calls the main functions of `RFM`, `CLV`, `Segmentation`, `Smart Insight`, and `Churn` modules sequentially, passing necessary context (like `firm_id`, `schema_name`).
5.  **Logging & Completion:** Logs the status (success/failure) of each module for each firm.

### Module Integration & Sequential Execution
Modules are tightly integrated and designed to run in a specific order managed by `main.py`:

-   **Sequence:** `RFM` -> `CLV` -> `Segmentation` -> `Smart Insight` -> `Churn` (Note: The relative order of Smart Insight and Churn might be configurable or context-dependent).
-   **Data Dependencies:**
    *   `RFM` and `CLV` share initial data preparation steps.
    *   `Segmentation` uses outputs from `RFM` and `CLV`.
    *   `Churn` has its own data prep but can use features enriched by other modules. Its results are often written to the same master customer table (`ANALYTIC_CUSTOMER`).
    *   `Smart Insight` aggregates results from all preceding modules.
-   **No Standalone Execution:** Modules are not intended to be run as independent scripts; orchestration via `main.py` is required.

### Containerization & Deployment Model
-   **Docker:** The platform is delivered as a `Docker` image, ensuring consistent execution across different Linux environments.
-   **Cron Scheduling:** Automated, periodic execution is achieved using standard Linux `cron` jobs that trigger the `main.py` script within the container.
-   **Configuration Management:** Database credentials and other sensitive settings are passed via environment variables during `docker run`. Business logic parameters (scheduling, thresholds) are managed within the `firms` table in the target Oracle database.

### Technical Infrastructure (Data Layer, Utilities)
-   **Data Layer (Oracle):** The platform interacts with a user-provided Oracle database. It expects specific input table structures (transactions, customers) within designated schemas (`{SCHEMA_NAME}`) and writes results back to the same database.
-   **Utilities Layer (`/app/utils/`):** Contains shared Python modules for:
    *   **Database Utilities (`db_utils.py`):** Handling Oracle connections, query execution, data insertion/updates.
    *   **General Utilities (`general_utils.py`):** Memory optimization, date/time functions, logging setup, outlier handling.
    *   **File Utilities (`file_utils.py`):** Reading/writing `.parquet` files.
    *   **Module-Specific Utilities:** Helper functions for RFM/CLV calculations, segmentation logic, Churn preprocessing, Smart Insight generation.

#### Central Workflow Diagram

```mermaid
flowchart TD
   A[Cron Job] --> B[Docker Container]
   B --> C[main.py: Central Orchestrator]
   C --> D{For Each Firm in `firms` table}
   D --> E{Check Schedule (`WORKING_DAY_PERIOD`) & Enabled Tasks}
   E -->|Tasks Due| F[Execute Analytics Pipeline for Firm]
   E -->|No Tasks Due| D
   F --> G[RFM Analysis]
   G --> H[CLV Calculation]
   H --> I[Segmentation]
   I --> J[Smart Insight]
   J --> K[Churn Prediction]
   K --> L[Write Results to Oracle DB (`ANALYTIC_CUSTOMER`, etc.)]
   L --> M[Log Completion/Errors]
   M --> D
```

---

## 3. Core Concepts & Shared Mechanisms

This section details the fundamental operating principles and shared components utilized across the CRM Analytics Platform modules.

### Data Sources & General Data Preparation Flow
-   **Primary Data Source:** The platform assumes the necessary input data (customer demographics, transaction history, loyalty interactions, firm metadata) resides within a user-managed Oracle database. Data for different tenants (firms) is typically expected in separate schemas (`{SCHEMA_NAME}`).
-   **Standard ETL + Analytics Flow:** Most modules follow a consistent data processing pattern:
    1.  **SQL Layer (Data Prep & `Feature Engineering`):** Executes comprehensive SQL queries against the target firm's schema. This involves cleaning, joining, aggregation, applying window functions, and calculating basic features (e.g., `Recency`, `Frequency`, `Monetary`, `DAYS_SINCE_LAST_TX`). Prepared data is often stored in intermediate (`staging`) database tables (e.g., `*_STG`, `ANALYTICAL_PROFILE`).
    2.  **Data Transfer (`Parquet`):** Large datasets prepared by SQL are frequently saved to the local filesystem (within the container, mapped via Docker volumes) as `.parquet` files for efficient processing in the Python layer, optimizing memory usage.
    3.  **Python Layer (Analytics Modeling & Processing):** Loads `.parquet` files into Python (`pandas` DataFrames). Performs more complex tasks: outlier management, data type casting, imputation, feature scaling (e.g., `MinMaxScaler`), ML model training/prediction (`LightGBM` for Churn), application of segmentation rules (`RFM`, `CLV`), statistical calculations, and structuring of final results.
    4.  **Results Storage:** Final analytics outputs (segment labels, scores, probabilities, metrics, `Smart Insights` text) generated in the Python layer are written back into target tables (e.g., `ANALYTIC_CUSTOMER`, `CHURN_PERFORMANCE_METRICS`) within the corresponding firm's schema in the Oracle database.

### Centralized Scheduling & Parameterization
The platform's flexibility and multi-tenant capability rely on a centralized configuration mechanism:

-   **`firms` Table:** A crucial configuration table expected within the Oracle database. It acts as the central registry for each tenant (firm) managed by the deployed platform instance. Each row typically contains:
    *   `FIRM_ID` and name.
    *   `SCHEMA_NAME`: The database schema containing the firm's data.
    *   Active/inactive status flag.
    *   `WORKING_DAY_PERIOD`: Defines the frequency of analytics runs (e.g., 1 for daily, 7 for weekly, 0 for inactive).
    *   Flags indicating which analytics modules (`RFM`, `CLV`, `Churn`, etc.) are enabled for the firm.
    *   Firm-specific business parameters, most notably `CHURN_THRESHOLD` (the number of inactivity days defining a churned customer).
-   **Dynamic Execution:** The `main.py` orchestrator reads the `firms` table at the start of each run. It triggers analyses only for active firms meeting the `WORKING_DAY_PERIOD` criteria. SQL queries and Python scripts dynamically use the firm's specific `{SCHEMA_NAME}` and parameters like `{CHURN_THRESHOLD}` during execution.
-   **Configuration Without Code Changes:** This design allows administrators or users to change analysis frequency, update the `churn` definition, or onboard new tenants simply by modifying records in the `firms` table, without needing to alter or redeploy the platform's code.

### Shared Utilities
To promote code reuse and consistency, common functionalities are centralized in utility modules under `/app/utils/`:

-   **Database Utilities (`db_utils.py`):** Manages Oracle connections, query execution, efficient data insertion (`insert`/`upsert` DataFrames), table operations (`drop`/`truncate`).
-   **General Utilities (`general_utils.py`):** Provides helpers for date/time manipulation, logging configuration, memory optimization techniques (e.g., `downcast_dtypes`), outlier detection/handling functions.
-   **Data Prep & Module Utilities (`data_prep_utils.py`, `segmentation_utils.py`, etc.):** Contain functions specific to certain analytics tasks but potentially reusable across modules (e.g., calculating RFM scores, applying CLV rules, generating `Smart Insight` text).
-   **File Utilities (`file_utils.py`):** Facilitates reading and writing `.parquet` files.

These shared utilities streamline development, enhance maintainability, and ensure consistency across the platform's different analytical components.

---

## 4. Analytics Modules (Core Features)

This section details the primary analytics modules that form the core functionality of the CRM Analytics Platform. Each module follows the general principles outlined in [Core Concepts & Shared Mechanisms](#core-concepts--shared-mechanisms).

### RFM (Recency, Frequency, Monetary) Segmentation Module

-   **Purpose:** To segment customers based on their transactional behavior: how recently they purchased (`Recency`), how often they purchase (`Frequency`), and how much they spend (`Monetary`). This is a foundational technique for personalized marketing and customer relationship management.
-   **Data Flow & Logic:**
    *   **SQL Layer:**
        *   Executes scripts like [`db_queries/segmentasyon/1-RFM.sql`](../db_queries/segmentasyon/1-RFM.sql).
        *   Extracts core data: `UNIQUE_CUSTOMER_ID`, `SON_ALV_TARIH` (last purchase date), transaction counts (`ALISVERIS_ADEDI`), total spend (`MUSTERI_TOPLAM_CIRO`).
        *   Calculates `RECENCY` (days since last transaction), `FREQUENCY` (count of valid transactions), `MONETARY` (sum of valid transaction amounts). May handle returns separately.
        *   Applies filters like time windows (e.g., last 2 years via `TIMED_ID_TRANSACTION >= ...`) and minimum spend thresholds (`SUM(AMOUNT_AFTER_DISCOUNT) > 15`).
        *   Can enrich data with demographics and additional behavioral metrics (discount usage, activity in last N months).
        *   Writes prepared data to intermediate tables (`RFM_STG`, `ANALYTICAL_PROFILE`, `ANALYTIC_ALL_DATA`).
    *   **Python Layer (`/app/segmentation/`):**
        *   Loads data prepared by SQL (often via `data/{schema_name}_all_data.parquet`).
        *   Assigns scores (typically 1-5) for `Recency`, `Frequency`, and `Monetary` based on relative rankings, often using `quantile` cuts.
        *   Combines these scores into an `RFM Score` (e.g., "555", "123").
        *   Maps RFM scores or individual R/F/M values to predefined segment labels (e.g., "VIP Customers", "Loyal Customers", "At Risk", "New Customers", "Hibernating") based on business rules. May also assign status labels like "Active", "At Risk", "Passive" based on `Recency` thresholds.
-   **Key Parameters/Configuration:**
    *   `Quantile` cutoffs for scoring (often dynamically calculated).
    *   `Recency` thresholds for status segments (e.g., Active < 90 days).
    *   Minimum transaction/spend filter values.
-   **Outputs & Storage:**
    *   Final RFM scores and segment labels are written per customer to the `ANALYTIC_CUSTOMER` table.
    *   Intermediate tables are typically cleared before the next run.
    *   A `.parquet` backup of the processed data is stored in `/data/`. ([See Storage & Access of Analytics Results](#storage--access-of-analytics-results))

### CLV (Customer Lifetime Value) Segmentation Module

-   **Purpose:** To estimate the total net profit or value a customer is expected to generate throughout their entire relationship with the business. This helps prioritize marketing spend, identify high-potential customers, and inform long-term strategy.
-   **Data Flow & Logic:**
    *   **SQL Layer:**
        *   Utilizes much of the same data preparation pipeline as RFM ([`db_queries/segmentasyon/2-Alv-profiles.sql`](../db_queries/segmentasyon/2-Alv-profiles.sql), [`db_queries/segmentasyon/4-all-data.sql`](../db_queries/segmentasyon/4-all-data.sql)), extracting necessary fields like `UNIQUE_CUSTOMER_ID`, `ILK_ODEME_TARIH` (first purchase), `SON_ALV_TARIH` (last purchase), `ALISVERIS_ADEDI` (transaction count), `MUSTERI_TOPLAM_CIRO` (total spend).
        *   Leverages intermediate tables like `ANALYTICAL_PROFILE` and `ANALYTIC_ALL_DATA`.
    *   **Python Layer (`/app/segmentation/`, `/app/utils/segmentation_utils.py`):**
        *   Loads prepared data (via `.parquet`).
        *   Calculates core `CLV` metrics for each customer:
            *   `customer_lifespan`: Days between first and last purchase.
            *   `avg_purchase_value`: Total spend / transaction count.
            *   `avg_purchase_frequency_rate`: Transaction count / customer lifespan (or normalized time unit).
            *   Applies a `CLV` calculation formula (e.g., a basic historical `CLV = avg_purchase_value * avg_purchase_frequency_rate * customer_lifespan`, potentially annualized `/ 365`). More sophisticated predictive models could be integrated here.
        *   Assigns customers to `CLV`-based segments using calculated CLV and potentially other metrics (e.g., purchase value, frequency, lifespan), often based on dynamic `quantile` cutoffs:
            *   **VIP Customer:** Top tier by CLV and average purchase value.
            *   **Loyal Customer:** High CLV and high purchase frequency.
            *   **Potential Growth:** Medium CLV but long customer lifespan.
            *   **One-timer:** Exactly one purchase (`ALISVERIS_ADEDI = 1`).
            *   **Risky Customer:** Low CLV or falling below key thresholds.
-   **Key Parameters/Configuration:**
    *   `Quantile` thresholds used for segmentation (e.g., top 20%, median).
    *   The specific `CLV` calculation formula employed.
-   **Outputs & Storage:**
    *   The calculated `CLV` value and assigned `CLV` segment label are written to the `ANALYTIC_CUSTOMER` table, alongside RFM results.
    *   A `.parquet` backup is stored. ([See Storage & Access of Analytics Results](#storage--access-of-analytics-results))

### Churn Prediction & Analysis Module

-   **Purpose:** To predict the likelihood of a customer becoming inactive or ceasing their relationship with the business (churn) within a defined future timeframe. This enables proactive retention efforts targeted at high-risk, high-value customers.
-   **Data Flow & Logic:**
    *   **SQL Layer (`/db_queries/churn/`):**
        *   **Customer Base (`ANALYTIC_CUSTOMER_BASE`):** Defines and cleans the eligible customer population for churn analysis.
        *   **Labeling:** Defines the `churn` event based on inactivity. Reads the firm-specific `{CHURN_THRESHOLD}` parameter (days of inactivity) from the `firms` table. Customers whose last transaction is older than this threshold are labeled `IS_CHURN = 1`, others `IS_CHURN = 0`.
        *   **`Feature Engineering`:** Calculates features potentially predictive of churn:
            *   `DAYS_SINCE_LAST_TRANSACTION` (`Recency`)
            *   `AVG_DAYS_BETWEEN_TRANSACTIONS`
            *   `AVG_SPENT`, `MAX_SPENT`
            *   `TOTAL_USED_POINT`
            *   `CUSTOMER_LIFETIME`
            *   `DISTINCT_TRANSACTIONS`
            *   Other demographic, transactional, or behavioral metrics.
        *   **Dataset Generation:**
            *   **Training Set:** Creates a dataset from a historical period containing both features and the `IS_CHURN` label. Saved as `TR_{SCHEMA_NAME}_churn_dataset.parquet`.
            *   **Prediction Set:** Creates a dataset for currently active customers (usually `IS_CHURN = 0` or unlabeled based on the prediction time window) containing only the features. Saved as `PR_{SCHEMA_NAME}_churn_dataset.parquet`.
    *   **Python Layer (`/app/churn/`):**
        *   Loads training and prediction `.parquet` files.
        *   Performs data cleaning, outlier handling (IQR-based), imputation, and categorical encoding.
        *   **Model Training:** Trains a `LightGBM` classification model using the training set. Uses stratified splitting to handle class imbalance. Hyperparameters can be tuned.
        *   **Model Evaluation:** Evaluates the trained model on a held-out test set using metrics like `AUC`, `accuracy`, `precision`, `recall`, `F1-score`, and confusion matrix. Results are stored in `CHURN_PERFORMANCE_METRICS`.
        *   **`Feature Importance`:** Calculates and stores feature importances in `CHURN_FEATURE_IMPORTANCES` for model interpretability.
        *   **Prediction:** Uses the trained model to predict the churn probability (a score between 0 and 1) for each customer in the prediction set.
        *   **Risk Categorization:** Maps probabilities to risk categories based on configurable thresholds:
            *   `low` risk: Probability < 0.6 (Default)
            *   `medium` risk: 0.6 <= Probability < 0.95 (Default)
            *   `high` risk: Probability >= 0.95 (Default)
        *   Assigns business-friendly labels ("CHURN", "CHURN RİSKİ YOK", "Low Risk", etc.).
-   **Key Parameters/Configuration:**
    *   **`CHURN_THRESHOLD`:** Defined per firm in the `firms` table; critical for labeling.
    *   Model hyperparameters (`LightGBM` settings).
    *   Probability thresholds for risk categories.
    *   List of features used in the model.
-   **Outputs & Storage:**
    *   Predicted churn probability, risk category, and labels are written/updated in the `ANALYTIC_CUSTOMER` table.
    *   Performance metrics are stored in `CHURN_PERFORMANCE_METRICS`.
    *   Feature importances are stored in `CHURN_FEATURE_IMPORTANCES`.
    *   Aggregated firm-level churn metrics might be stored in `CHURN_FIRM_BASED`.
    *   Training/prediction `.parquet` files and the serialized model (`.pkl`) are saved. ([See Storage & Access of Analytics Results](#storage--access-of-analytics-results))

### Smart Insight Module

-   **Purpose:** To automatically generate business-readable summaries and interpretations of the overall customer portfolio health, leveraging the outputs from the `RFM`, `CLV`, and `Churn` modules. It aims to bridge the gap between technical analytics results and actionable insights for non-technical stakeholders.
-   **Data Flow & Logic:**
    *   **Input Data:** Primarily uses the `ANALYTIC_CUSTOMER` table, which contains the consolidated results from segmentation and churn analyses.
    *   **SQL Layer (`db_queries/Insight/overall-firm.sql`):**
        *   Aggregates metrics from `ANALYTIC_CUSTOMER` at the firm level:
            *   Total customer counts (active/inactive).
            *   Median/Average `Recency`, `Frequency`, `Monetary`, `CLV`, `customer_lifespan`.
            *   Segment distributions (% VIP, % Loyal, % At Risk, etc.).
            *   Churn risk distributions (% Low, % Medium, % High Risk).
            *   Key `KPI`s like retention rates, conversion rates (e.g., one-timers to active), new customer share.
            *   Estimated revenue at risk from churn (e.g., count of high-risk customers * avg spend).
        *   These aggregated metrics are often written to standardized columns (`P1`, `P2`, ... `P_N`) in the `ANALYTIC_FIRM_BASED` table.
    *   **Python Layer (`/app/utils/smart_insight_utils.py`):**
        *   Reads the firm-level aggregated metrics calculated by SQL.
        *   **Automated Text Generation (`Natural Language Generation`):** Uses predefined templates and business rules to construct a narrative summary (`SMART_INSIGHT` text, currently in Turkish in the example implementation) based on the calculated metrics. This text typically covers:
            *   Overall portfolio health and history summary.
            *   Customer activity and loyalty/retention performance highlights.
            *   Key segment sizes, risks, and opportunities.
            *   Tailored recommendations based on metric patterns (e.g., commentary triggered by high churn risk or low VIP conversion).
        *   An example output can be found in [`docs/smartInsight-ornek.docx`](../docs/smartInsight-ornek.docx).
-   **Key Parameters/Configuration:**
    *   Relies heavily on the outputs of other modules.
    *   The text generation templates and business rules are defined within the Python code (`smart_insight_utils.py`).
-   **Outputs & Storage:**
    *   The generated `SMART_INSIGHT` text and the supporting aggregated metrics are written to the `ANALYTIC_FIRM_BASED` table for the corresponding firm and analysis period. ([See Storage & Access of Analytics Results](#storage--access-of-analytics-results))
-   **Future Direction:** The vision for this module within the open-source project could involve evolving it into an AI-powered conversational analytics interface (chatbot) capable of answering natural language questions about CRM metrics, potentially integrating LLMs and offering more dynamic strategy recommendations.

---

## 5. Storage & Access of Analytics Results

The CRM Analytics Platform stores its outputs in a structured manner within the target Oracle database, making them readily accessible for various purposes.

### Main Output Tables (`ANALYTIC_CUSTOMER`, `ANALYTIC_FIRM_BASED`)

-   **`ANALYTIC_CUSTOMER`:**
    *   **Scope:** The primary customer-level output table, consolidating results from multiple modules. Each row represents a unique customer (`UNIQUE_CUSTOMER_ID`).
    *   **Content:** Contains `RFM` scores/segments, `CLV` value/segment, `Churn` probability/risk category, supporting metrics, demographic enrichments, and metadata (timestamps, run IDs).
    *   **Usage:** Serves as the "golden record" source for BI dashboards (customer drill-downs), targeted marketing list generation, and feeding other CRM systems. Typically updated via `overwrite` or `upsert` logic on each run.

-   **`ANALYTIC_FIRM_BASED`:**
    *   **Scope:** Stores aggregated firm-level (portfolio-wide) metrics and the outputs of the `Smart Insight` module. Each row represents a summary for a specific firm (`FIRM_ID`) and analysis period.
    *   **Content:** Includes total customer counts, average/median behavioral metrics, segment distributions, churn risk breakdowns, calculated `KPI`s, and the natural language `SMART_INSIGHT` text. Metrics are often stored in standardized `P_1`...`P_N` columns.
    *   **Usage:** Feeds executive dashboards for monitoring overall portfolio health, tracking trends, and displaying automated insights. Usually updated by appending (`append`) a new row for each analysis run.

### Performance & Audit Tables
Specific tables track model performance and aid interpretability, especially for the `Churn` module:

-   **`CHURN_PERFORMANCE_METRICS`:** Records key performance indicators (`AUC`, `accuracy`, `F1`, etc.) for each `Churn` model training run, along with model identifiers and timestamps. Useful for monitoring model degradation and auditing.
-   **`CHURN_FEATURE_IMPORTANCES`:** Stores the calculated importance scores for each feature used in the trained `Churn` model (`LightGBM`). Aids in understanding model behavior and identifying key drivers of churn.

### Intermediate Storage (`.parquet`)
-   **Purpose:** `.parquet` files are used primarily for efficient data transfer between the SQL preparation layer and the Python analytics layer, especially for large datasets. This optimizes I/O and memory usage.
-   **Usage:** Files like `data/{schema_name}_all_data.parquet`, `TR_{schema_name}_churn_dataset.parquet`, and `PR_{schema_name}_churn_dataset.parquet` are stored temporarily or as backups on the filesystem volume mapped to the container (e.g., under `/data/`).

### BI (Business Intelligence) Integration
-   **Direct Connection:** Since all final results reside in standard Oracle tables, BI tools like Qlik Sense, Power BI, Tableau, etc., can connect directly to the database using standard Oracle connectors.
-   **Data Readiness:** The output tables (`ANALYTIC_CUSTOMER`, `ANALYTIC_FIRM_BASED`) provide pre-processed, structured data ready for visualization and reporting, reducing the data preparation effort required within the BI tool itself.
-   **Freshness:** Regular, automated runs of the analytics pipeline ensure that the data feeding the BI dashboards reflects the latest available insights and customer states according to the configured schedule.

---

## 6. Setup & Administration

This section provides guidance on setting up, deploying, and managing the CRM Analytics Platform.

### Prerequisites
-   **Operating System:** A Linux server capable of running Docker.
-   **Software:**
    *   Python 3.8+.
    *   Docker Engine installed and running.
    *   `git` client (for cloning the repository).
-   **Database Access:**
    *   Network connectivity from the Docker host to the target Oracle database.
    *   An Oracle database user with appropriate privileges (SELECT on source tables, CREATE/INSERT/UPDATE/DELETE on target/staging tables within the relevant schemas).
    *   Oracle connection details: username, password, host, port, service name/SID.
-   **Source Code:** Access to clone the project repository from [https://github.com/esmaascioglu/CRM-Analytics-Module](https://github.com/esmaascioglu/CRM-Analytics-Module).

### Installation & Deployment Steps

The platform is deployed as a Docker container.

#### 1. Clone Repository```bash
# Navigate to your desired deployment directory
cd /path/to/deploy/directory
# Clone the repository
git clone https://github.com/esmaascioglu/CRM-Analytics-Module.git
cd CRM-Analytics-Module
```

#### 2. Build Docker Image
From the root directory of the cloned repository (where the `Dockerfile` is located):
```bash
docker build -t crm-analytics-platform:latest .
```
This creates a Docker image named `crm-analytics-platform` tagged as `latest`.

#### 3. Run Docker Container
**Crucially, you must provide your specific Oracle database connection details and map volumes for persistent logs and data.**```bash
# Stop and remove any previous container instance (optional)
docker stop crm-analytics-platform-container || true
docker rm crm-analytics-platform-container || true

# Run the new container
docker run -d \
  --name crm-analytics-platform-container \
  --restart unless-stopped \
  -v /path/on/host/for/logs:/app/logs \
  -v /path/on/host/for/data:/app/data \
  -e ORACLE_USER="YOUR_DB_USER" \
  -e ORACLE_PASSWORD="YOUR_DB_PASSWORD" \
  -e ORACLE_HOST="YOUR_DB_HOST_OR_IP" \
  -e ORACLE_PORT="1521" \
  -e ORACLE_SERVICE="YOUR_DB_SERVICE_NAME" \
  # Add any other required environment variables here
  crm-analytics-platform:latest
```
-   Replace placeholders (`YOUR_DB_...`, `/path/on/host/...`) with your actual values.
-   `-d`: Run in detached mode.
-   `--name`: Assign a recognizable name to the container.
-   `--restart unless-stopped`: Ensure the container restarts automatically if the server reboots.
-   `-v ...:/app/logs`: Map host directory to container's log directory for persistence.
-   `-v ...:/app/data`: Map host directory to container's data directory for `.parquet` files.
-   `-e VARIABLE="value"`: **Supply Oracle credentials and potentially other configurations via environment variables.** Using an `--env-file` is also a good practice for managing multiple variables.

### Automated Updates (`restart.sh` & `cron`)

A common pattern to keep the deployment updated with the latest code changes from the repository involves a helper script and a `cron` job.

#### 1. Create `restart.sh` Script
Create a script (e.g., in the deployment directory) like the one below. **Remember to customize paths and environment variables.**

```bash
#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status.

LOG_FILE="/var/log/crm_platform_restart.log"
REPO_DIR="/path/to/CRM-Analytics-Module" # CHANGE THIS
IMAGE_NAME="crm-analytics-platform:latest"
CONTAINER_NAME="crm-analytics-platform-container"
HOST_LOG_DIR="/path/on/host/for/logs" # CHANGE THIS
HOST_DATA_DIR="/path/on/host/for/data" # CHANGE THIS

echo "[`date '+%Y-%m-%d %H:%M:%S'`] CRM Analytics Platform update and restart initiated" >> $LOG_FILE

# Navigate to repository
cd $REPO_DIR || { echo "[`date '+%Y-%m-%d %H:%M:%S'`] ERROR: Failed to cd into $REPO_DIR" >> $LOG_FILE; exit 1; }

# Pull latest code
echo "[`date '+%Y-%m-%d %H:%M:%S'`] Performing git pull..." >> $LOG_FILE
git pull origin main >> $LOG_FILE 2>&1 || echo "[`date '+%Y-%m-%d %H:%M:%S'`] WARNING: Git pull finished with non-zero status." >> $LOG_FILE

# Build Docker image
echo "[`date '+%Y-%m-%d %H:%M:%S'`] Building Docker image ($IMAGE_NAME)..." >> $LOG_FILE
docker build -t $IMAGE_NAME . >> $LOG_FILE 2>&1
if [ $? -ne 0 ]; then
    echo "[`date '+%Y-%m-%d %H:%M:%S'`] ERROR: Docker build failed." >> $LOG_FILE
    exit 1
fi

# Stop and remove existing container
echo "[`date '+%Y-%m-%d %H:%M:%S'`] Stopping and removing old container ($CONTAINER_NAME)..." >> $LOG_FILE
docker stop $CONTAINER_NAME >> $LOG_FILE 2>&1 || true
docker rm $CONTAINER_NAME >> $LOG_FILE 2>&1 || true

# Start new container (Include ALL necessary -v and -e flags)
echo "[`date '+%Y-%m-%d %H:%M:%S'`] Starting new container ($CONTAINER_NAME)..." >> $LOG_FILE
docker run -d \
  --name $CONTAINER_NAME \
  --restart unless-stopped \
  -v ${HOST_LOG_DIR}:/app/logs \
  -v ${HOST_DATA_DIR}:/app/data \
  -e ORACLE_USER="YOUR_DB_USER" \
  -e ORACLE_PASSWORD="YOUR_DB_PASSWORD" \
  -e ORACLE_HOST="YOUR_DB_HOST_OR_IP" \
  -e ORACLE_PORT="1521" \
  -e ORACLE_SERVICE="YOUR_DB_SERVICE_NAME" \
  # Add other environment variables \
  $IMAGE_NAME >> $LOG_FILE 2>&1
if [ $? -ne 0 ]; then
    echo "[`date '+%Y-%m-%d %H:%M:%S'`] ERROR: Failed to start new container." >> $LOG_FILE
    exit 1
fi

echo "[`date '+%Y-%m-%d %H:%M:%S'`] Update and restart process completed." >> $LOG_FILE
exit 0
```
Make the script executable:
```bash
chmod +x restart.sh
```

#### 2. Setup Cron Job
Schedule the `restart.sh` script to run periodically (e.g., daily at 2 AM):
```bash
crontab -e
```
Add a line like this (use the full path to your script):
```crontab
0 2 * * * /path/to/your/restart.sh
```

### Monitoring & Troubleshooting

-   **Container Status:** Check if the container is running:
    ```bash
    docker ps | grep crm-analytics-platform-container
    ```
    Check all containers (including stopped):
    ```bash
    docker ps -a | grep crm-analytics-platform-container
    ```
-   **Container Logs:** View the real-time output of the `main.py` script and modules:
    ```bash
    docker logs crm-analytics-platform-container
    ```
    Follow logs continuously:
    ```bash
    docker logs -f crm-analytics-platform-container
    ```
    Check persistent log files in the mapped host directory (`/path/on/host/for/logs`).
-   **Restart Script Logs:** Check the log file defined in `restart.sh`:
    ```bash
    cat /var/log/crm_platform_restart.log
    ```
-   **Common Issues:**
    *   **Permissions:** Ensure the user running `cron` has permissions to execute Docker commands (often requires adding the user to the `docker` group).
    *   **Database Connectivity:** Verify network access to the Oracle DB from the Docker host. Double-check credentials and connection parameters passed as environment variables. Test connectivity from within the container if necessary (`docker exec -it <container_name> bash` then try to connect).
    *   **Disk Space:** Monitor disk usage on the host, especially for Docker images/layers and the mapped log/data volumes.
    *   **Code Errors:** Look for Python `traceback` errors in the `docker logs`.

### Quick Start Example (Manual Trigger)
To manually trigger the analytics pipeline for a specific firm (tenant) for testing purposes:
```bash
# Replace YOUR_FIRM_ID with the actual ID from your 'firms' table
docker exec crm-analytics-platform-container python /app/main.py --firm_id=YOUR_FIRM_ID
```
Omitting the `--firm_id` argument will typically trigger the process for all firms scheduled according to the `firms` table logic within `main.py`.

---

## 7. Extensibility & Customization

The CRM Analytics Platform is designed with extensibility in mind, allowing developers and implementers to adapt it to specific needs.

-   **Adding New Analytics Modules:**
    *   The modular structure under `/app/` and the central orchestration in `main.py` facilitate adding new analytical capabilities (e.g., recommendation engine, propensity modeling, next best offer).
    *   A new module can be created in its own directory (e.g., `/app/recommendations/`) with its specific Python logic and potentially corresponding SQL queries (`/db_queries/recommendations/`).
    *   It needs to be integrated into the execution sequence within `main.py`.
    *   It can leverage the existing shared utilities (`db_utils`, `general_utils`, etc.).

-   **Customizing Existing Modules:**
    *   **Segmentation Logic:** Modify the rules for assigning `RFM` or `CLV` segments by editing the relevant Python code (e.g., in `/app/utils/segmentation_utils.py`) or the underlying SQL queries. Different `quantile` ranges or business rules can be implemented.
    *   **Churn Model:**
        *   **Features:** Add or remove predictive features by updating the feature engineering SQL (`/db_queries/churn/`) and the corresponding Python preprocessing steps (`/app/churn/`).
        *   **Model Tuning:** Adjust `LightGBM` hyperparameters in `/app/churn/modelling.py` or experiment with entirely different classification algorithms.
        *   **Thresholds:** Modify the probability thresholds used to categorize customers into low/medium/high churn risk segments.
    *   **Smart Insights:** Enhance the automated text generation logic in `/app/utils/smart_insight_utils.py` to include new metrics, apply different interpretation rules, or generate outputs in other languages. Add new KPIs to `overall-firm.sql`.

-   **Parameter-Driven Customization:** As detailed in [Centralized Scheduling & Parameterization](#centralized-scheduling--parameterization), key operational parameters like analysis frequency and the `CHURN_THRESHOLD` can be configured per tenant via the `firms` database table without code changes.

-   **Data Source Integration:** While designed for Oracle, the data ingestion logic (primarily in `db_utils.py` and SQL queries) could potentially be adapted or extended to integrate data from other sources if required, though this would likely involve more significant code changes.

This flexibility allows the open-source platform to serve as a robust starting point that can be tailored and expanded upon by the community or individual implementers.

---

## 8. Data Security & Compliance Considerations

While deploying the CRM Analytics Platform, implementers are responsible for ensuring data security and compliance within their own environment. The platform's design incorporates elements to facilitate this, but ultimate responsibility lies with the user. Key considerations include:

-   **Access Control:** Database access relies entirely on the credentials provided via environment variables during container startup. It is crucial to use dedicated database users with the minimum necessary privileges (least privilege principle) for the schemas the platform needs to access.
-   **Data Isolation:** The use of `Docker` provides process-level isolation. However, network access rules and database-level security must be properly configured by the implementer.
-   **Sensitive Data Handling (PII):**
    *   The implementer is responsible for ensuring compliance with regulations like GDPR, CCPA, KVKK, etc., regarding Personally Identifiable Information (PII).
    *   The platform itself processes the data provided to it. Implementers should avoid feeding unnecessary PII into the source tables or implement anonymization/pseudonymization techniques upstream if required by their policies.
    *   Analytical outputs (segments, scores) should be handled according to the organization's data governance policies.
-   **Secure Configuration:** **Never hardcode credentials** or other sensitive information directly in the code or Dockerfile. Use environment variables (passed securely during `docker run`) or consider using secrets management tools integrated with your container orchestration platform (if applicable).
-   **Data Transmission:** Ensure network communication between the Docker host and the Oracle database is secured according to organizational standards (e.g., using encrypted connections if necessary and configured correctly on the Oracle side).
-   **Logging and Auditing:** The platform generates logs detailing its operations. Implementers should ensure these logs (stored via mapped volumes) are managed securely and retained according to audit and compliance requirements.
-   **Infrastructure Security:** The underlying security of the Linux host running Docker and the Oracle database server itself (patching, firewalling, access controls) is the responsibility of the implementer.

By carefully managing credentials, data access, configuration, and the underlying infrastructure, implementers can deploy the CRM Analytics Platform in a secure and compliant manner.

---

## 9. FAQ (Frequently Asked Questions)

Answers to common technical and business questions regarding the CRM Analytics Platform.

**General & Architecture**

1.  **What are the core analytics modules of the platform?**
    *   `RFM` & `CLV` (Segmentation), `Churn Prediction`, and `Smart Insight` generation. ([See Analytics Modules (Core Features)](#analytics-modules-core-features))

2.  **How does the platform work? What's the main workflow?**
    *   A `cron` job triggers `main.py` in the Docker container. `main.py` reads the `firms` table for scheduling, then sequentially runs the enabled modules (`RFM`, `CLV`, etc.) for each due firm. Modules use SQL for data prep and Python for analysis, writing results back to Oracle DB. ([See System Architecture](#system-architecture))

3.  **How is a new B2B client/tenant configured in a deployment?**
    *   By adding a new record to the `firms` table in the Oracle database, specifying the `SCHEMA_NAME`, `WORKING_DAY_PERIOD`, enabled modules, `CHURN_THRESHOLD`, etc. No code change is needed. ([See Centralized Scheduling & Parameterization](#centralized-scheduling--parameterization))

4.  **Can the system perform both historical (batch) and daily (incremental) analyses?**
    *   Yes. The design supports both. Historical runs can be triggered manually or configured with specific date ranges (requires potential minor adjustments in SQL/logic depending on exact need), while scheduled `cron` runs typically process the latest data incrementally.

**Parameters & Customization**

5.  **Which parameters can implementers/users configure without code changes?**
    *   Primarily via the `firms` table: analysis frequency (`WORKING_DAY_PERIOD`), churn inactivity definition (`CHURN_THRESHOLD`), potentially minimum spend thresholds used in SQL filters. ([See Centralized Scheduling & Parameterization](#centralized-scheduling--parameterization))

6.  **How is the "churned" customer definition determined and can it be changed?**
    *   It's defined by the `CHURN_THRESHOLD` value (number of inactivity days) set per firm in the `firms` table. Changing the value in the table redefines churn for subsequent runs. ([See Churn Prediction & Analysis Module](#churn-prediction--analysis-module))

7.  **Can analysis periods/frequencies be customized per tenant?**
    *   Yes, using the `WORKING_DAY_PERIOD` field in the `firms` table. ([See Centralized Scheduling & Parameterization](#centralized-scheduling--parameterization))

8.  **Can new business rules or metrics be added to existing modules?**
    *   Yes, the platform's modular nature allows modification of SQL queries (`/db_queries/`) or Python logic (`/app/`) to incorporate new metrics or rules. ([See Extensibility & Customization](#extensibility--customization))

9.  **Can I adapt the segmentation logic or Churn model for specific business needs?**
    *   Yes. Segmentation rules are in Python/SQL, Churn model features are defined in SQL, and model parameters/thresholds are set in Python, all of which can be modified. ([See Extensibility & Customization](#extensibility--customization))

**Data & Results**

10. **What time window of data is used for the analyses?**
    *   **RFM/CLV:** Often use a rolling window (e.g., last 24 months) defined in SQL, but calculate metrics based on full history and recent periods (1/3/6/12 months).
    *   **Churn:** Features are typically engineered over a defined historical period. Labeling depends on the dynamic `CHURN_THRESHOLD`. Prediction applies to the current active base.
    *   **Smart Insight:** Generally references the latest available results and often focuses on rolling 12-month trends.

11. **How are customers with minimal or one-off activity handled?**
    *   SQL queries can optionally filter out customers below a minimum spend/transaction threshold.
    *   CLV segmentation explicitly identifies "One-timer" customers.
    *   Churn models might implicitly exclude customers with insufficient history depending on feature engineering.

12. **Are demographic or program-specific features used in the analysis?**
    *   Yes, if available in the source Oracle tables, they can be pulled during the SQL preparation phase and used for feature engineering or segmentation enrichment. ([See RFM Module](#rfm-recency-frequency-monetary-segmentation-module))

13. **How are analytics results (segments, scores) updated? Are old values overwritten?**
    *   Customer-level tables like `ANALYTIC_CUSTOMER` are typically updated using an `overwrite` or `upsert` strategy based on `UNIQUE_CUSTOMER_ID`.
    *   Time-series or run-based tables like `ANALYTIC_FIRM_BASED` or `CHURN_PERFORMANCE_METRICS` usually `append` new records for each run, preserving history. Timestamps and run IDs facilitate tracking.

14. **How can I identify which model or thresholds were applied for a specific customer/period?**
    *   Output tables often include timestamps, model IDs (if applicable), and potentially logged parameter values, allowing traceability back to the specific run configuration.

15. **Can the analytics results be integrated with BI tools (Qlik, Power BI, etc.)?**
    *   Yes, results are stored in standard Oracle tables, enabling direct connection from BI tools via standard database connectors. ([See Storage & Access of Analytics Results](#storage--access-of-analytics-results))

**Operation & Technical**

16. **Can the system handle large datasets (millions of customers)? How is scalability achieved?**
    *   The platform is designed for scale. This is achieved through optimized SQL (window functions, filtering), use of the efficient `.parquet` format for data transfer, memory optimization techniques in Python (`pandas`), and the use of scalable libraries like `LightGBM`.

17. **What happens if the analytics process encounters an error?**
    *   The application includes error handling (`try-except`) and detailed logging. If an error occurs for a specific firm/module, the process for that firm typically stops, logs the error, and the orchestrator moves to the next firm. Database transactions are often rolled back on error to maintain data integrity.

18. **What happens if a required parameter (e.g., `CHURN_THRESHOLD`) is missing?**
    *   The platform should validate required parameters at runtime. If a critical parameter from the `firms` table or environment is missing, the run for that firm will fail safely with a logged error, preventing execution with invalid configuration.

19. **How can I (the user/implementer) validate that the analytics results are accurate?**
    *   Check model performance metrics in `CHURN_PERFORMANCE_METRICS`.
    *   Review feature importances in `CHURN_FEATURE_IMPORTANCES` for model interpretability.
    *   Manually verify calculations or segment assignments on sample data.
    *   Compare results against known historical outcomes or business intuition.
    *   Examine the SQL and Python logic directly for calculation methodology.

20. **Does the system log all analytic runs and errors?**
    *   Yes, the platform's code includes comprehensive logging for execution steps, status, errors, and key outputs, aiding both troubleshooting and auditing. ([See Data Security & Compliance Considerations](#data-security--compliance-considerations))

---

## 10. Release Notes & Roadmap

*(This section serves as a placeholder for the open-source project's evolution.)*

-   **Potential Roadmap / Future Goals:**
    *   Enhance `Smart Insight` module with conversational AI (chatbot) capabilities.
    *   Integrate more advanced predictive `CLV` models.
    *   Add a module for product recommendations or next best action.
    *   Develop API endpoints for triggering runs or retrieving results.
    *   Explore integration with other data sources beyond Oracle.
    *   Create a simple UI for managing the `firms` table parameters.
-   **Release Notes:**
    *   *(Track major changes, features, and bug fixes per version here as the project evolves.)*

---

## 11. Database Tables & Data Sources Overview

This section summarizes the key database tables involved in the platform's operation, both as inputs (expected structure) and outputs (created/populated by the modules). Note that `{schema_name}` refers to the specific Oracle schema configured for each tenant/firm in the `firms` table.

### Configuration Table

| Table Name | Description                                                                 | Key Columns (Examples)                                                              | Usage Point        |
|------------|-----------------------------------------------------------------------------|------------------------------------------------------------------------------------|--------------------|
| `firms`    | Central configuration registry for each tenant/firm managed by the platform. | `FIRM_ID`, `SCHEMA_NAME`, `IS_ACTIVE`, `WORKING_DAY_PERIOD`, `CHURN_THRESHOLD`, ... | `main.py` (Reads) |

### Input Tables (Expected in `{schema_name}`)

*(Note: These are representative examples. The actual required input tables and columns depend on the specific SQL queries implemented in `/db_queries/`.)*

| Table Name (Example) | Description                       | Key Columns (Examples)                                         | Used By Modules |
|----------------------|-----------------------------------|----------------------------------------------------------------|-----------------|
| `CUSTOMERS`          | Customer demographic information. | `UNIQUE_CUSTOMER_ID`, `BIRTH_DATE`, `GENDER`, `CITY`, ...      | RFM, CLV, Churn |
| `TRANSACTIONS`       | Customer transaction records.     | `TRANSACTION_ID`, `UNIQUE_CUSTOMER_ID`, `TRX_DATE`, `AMOUNT`, `TRX_STATE_ID` (sale/return), `IS_DELETED`, `TIMED_ID_TRANSACTION`, ... | RFM, CLV, Churn |
| `LOYALTY_POINTS`     | Loyalty program point activity.   | `UNIQUE_CUSTOMER_ID`, `POINTS_EARNED`, `POINTS_USED`, `DATE`, ... | RFM, CLV, Churn |
| `PROGRAMS`           | Loyalty program definitions.      | `DWH_PROGRAM_ID`, `PROGRAM_NAME`, ...                          | RFM, CLV, Churn |

### Staging / Intermediate Tables (Created/Used within `{schema_name}`)

| Table Name            | Description                                                                 | Usage Point                 | Typically Cleared? |
|-----------------------|-----------------------------------------------------------------------------|-----------------------------|--------------------|
| `RFM_STG`             | Intermediate staging table for RFM calculations.                            | SQL Layer (RFM Prep)        | Yes                |
| `ANALYTICAL_PROFILE`  | Stores calculated profile metrics before final aggregation.                 | SQL Layer (RFM/CLV Prep)    | Yes                |
| `ANALYTIC_ALL_DATA`   | Holds fully integrated and enriched data before Python processing (RFM/CLV). | SQL Layer (RFM/CLV Prep)    | Yes                |
| `ANALYTIC_CUSTOMER_BASE`| Cleaned base population of customers eligible for Churn analysis.         | SQL Layer (Churn Prep)      | Yes                |

### Output Tables (Created/Populated within `{schema_name}`)

| Table Name                  | Description                                                                  | Key Columns (Examples)                                                                     | Populated By Modules | Update Strategy     |
|-----------------------------|------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------|----------------------|---------------------|
| `ANALYTIC_CUSTOMER`         | **Main customer-level output.** Consolidates segments, scores, probabilities. | `UNIQUE_CUSTOMER_ID`, `RFM_SCORE`, `RFM_SEGMENT`, `CLV_VALUE`, `CLV_SEGMENT`, `CHURN_PROBABILITY`, `CHURN_RISK_CATEGORY`, `RUN_DATE`, ... | RFM, CLV, Churn      | Overwrite / Upsert  |
| `ANALYTIC_FIRM_BASED`       | **Main firm-level output.** Aggregated metrics and automated insights.       | `FIRM_ID`, `RUN_DATE`, `METRIC_P1`, `METRIC_P2`, ..., `SMART_INSIGHT`                        | Smart Insight        | Append              |
| `CHURN_PERFORMANCE_METRICS` | Stores performance metrics for each Churn model training run.              | `RUN_ID`, `MODEL_ID`, `AUC`, `ACCURACY`, `F1_SCORE`, `RUN_DATE`, ...                       | Churn                | Append              |
| `CHURN_FEATURE_IMPORTANCES` | Stores feature importance scores for each trained Churn model.             | `RUN_ID`, `MODEL_ID`, `FEATURE_NAME`, `IMPORTANCE_SCORE`, `RUN_DATE`, ...                  | Churn                | Append              |
| `CHURN_FIRM_BASED`          | (Optional) Stores aggregated Churn-specific metrics at the firm level.     | `FIRM_ID`, `RUN_DATE`, `HIGH_RISK_COUNT`, `HIGH_RISK_PERCENT`, ...                         | Churn (potentially)  | Append or Overwrite |

### Local Filesystem Data (`.parquet`, `.pkl` - Mapped via Docker Volume)

| File Path Pattern                           | Description                                                            | Usage Point                    |
|---------------------------------------------|------------------------------------------------------------------------|--------------------------------|
| `/data/{schema_name}_all_data.parquet`      | Parquet export of `ANALYTIC_ALL_DATA` for RFM/CLV Python processing.   | SQL Layer (Output) -> Python Layer (Input) |
| `/data/TR_{schema_name}_churn_dataset.parquet`| Training dataset for the Churn model.                                  | SQL Layer (Output) -> Python Layer (Input - Train) |
| `/data/PR_{schema_name}_churn_dataset.parquet`| Prediction dataset for the Churn model.                                | SQL Layer (Output) -> Python Layer (Input - Predict) |
| `/models/churn_model_{schema_name}.pkl`      | Serialized (saved) trained Churn model object.                         | Python Layer (Output - Train, Input - Predict) |

---
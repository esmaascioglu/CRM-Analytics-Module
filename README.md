# CRM Analytics Platform

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

An open-source, containerized platform for automating common CRM analytics workflows (RFM, CLV, Churn Prediction, Smart Insights) using Python and Oracle SQL, designed particularly for loyalty program data.

---

## Overview

The CRM Analytics Platform provides an end-to-end, automated solution for deriving valuable customer insights from transactional and loyalty data stored in an Oracle database. It integrates data preparation, feature engineering, machine learning (for churn prediction), and insight generation into a deployable Docker container.

This platform helps organizations understand customer behavior, predict churn, segment their customer base effectively, and receive automated portfolio summaries, enabling data-driven marketing and retention strategies.

## Key Features

*   **Automated Analytics Pipelines:** Executes RFM, CLV, Churn Prediction, and Smart Insight generation based on a schedule defined in the database.
*   **Customer Segmentation:**
    *   **RFM:** Segments customers based on Recency, Frequency, and Monetary value.
    *   **CLV:** Calculates Customer Lifetime Value and segments accordingly (e.g., VIP, Loyal, Potential Growth).
*   **Churn Prediction:** Uses LightGBM to predict customer churn probability and assign risk categories (Low, Medium, High). Includes model performance tracking.
*   **Smart Insights:** Automatically generates natural language summaries (currently Turkish example) of portfolio health, key metrics, and actionable recommendations based on analytics results.
*   **B2B / Multi-Tenant Ready:** Designed to handle multiple clients or business units (tenants) via database configuration (`firms` table), processing data within separate schemas.
*   **Configurable:** Key business parameters like analysis frequency (`WORKING_DAY_PERIOD`) and churn definition (`CHURN_THRESHOLD`) are centrally managed in the database without code changes.
*   **Scalable:** Built to handle large customer datasets using efficient data processing techniques (SQL optimization, Parquet, Pandas, LightGBM).
*   **Containerized:** Delivered as a Docker image for easy deployment and consistent execution.
*   **Extensible:** Modular architecture allows for customization and addition of new analytics modules.

## Core Technologies

*   **Python 3.8+**
*   **Pandas:** Data manipulation and analysis.
*   **LightGBM:** Machine learning model for churn prediction.
*   **Oracle SQL:** Data storage, retrieval, and initial preparation via user-provided queries.
*   **Docker:** Containerization for deployment.
*   **Cron:** (Recommended) For scheduling automated runs.

## Getting Started

### Prerequisites

*   Linux Server
*   Docker Engine installed
*   Python 3.8+ (primarily for potential local development/testing)
*   Access to an Oracle Database instance
*   `git` client

### Installation & Quick Start

1.  **Clone the Repository:**
    ```bash
    git clone https://github.com/esmaascioglu/CRM-Analytics-Module.git
    cd CRM-Analytics-Module
    ```

2.  **Build the Docker Image:**
    ```bash
    docker build -t crm-analytics-platform:latest .
    ```

3.  **Run the Docker Container:**
    *   **Important:** You MUST provide your Oracle database connection details as environment variables (`-e` flag or `--env-file`).
    *   You MUST map host directories to `/app/logs` and `/app/data` inside the container using the `-v` flag for persistent logging and data storage (e.g., Parquet files).

    ```bash
    # --- Example docker run command ---
    # --- Replace placeholders with your actual values ---
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
      crm-analytics-platform:latest
    ```

4.  **Database Setup:** Ensure your Oracle database contains the necessary source tables (customers, transactions, etc.) and the `firms` configuration table as described in the full documentation.

5.  **Automation:** Configure a `cron` job on the host machine to trigger the `main.py` script inside the running container periodically (e.g., `docker exec crm-analytics-platform-container python /app/main.py`). See the `restart.sh` script example in the full documentation for automated updates and restarts.

## Documentation

**For detailed information on architecture, module specifics, configuration, database schemas, and advanced usage, please refer to the [Full Documentation](docs/index.md).**

## Configuration

*   **Database Connection:** Provided via environment variables during `docker run`.
*   **Scheduling & Business Parameters:** Managed per-tenant via the `firms` table in your Oracle database. This includes:
    *   `SCHEMA_NAME` (Tenant's data schema)
    *   `WORKING_DAY_PERIOD` (Analysis frequency)
    *   `CHURN_THRESHOLD` (Days of inactivity defining churn)
    *   Enabled modules flags
    *   See the [Full Documentation](docs/index.md) for details on the `firms` table structure and parameters.

## Contributing

Contributions are welcome! Please read the contribution guidelines (TODO: Create CONTRIBUTING.md) before submitting pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details. <!-- Make sure you add a LICENSE file with your chosen license (e.g., MIT) -->
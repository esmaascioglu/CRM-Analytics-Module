
SELECT 
    TOTAL.FIRM_ID,
    
    -- Existing Metrics
    EXTRACT(YEAR FROM TO_DATE(MIN(TOTAL.P1), 'YYYYMMDD')) AS earliest_trn_year,
    EXTRACT(MONTH FROM TO_DATE(MIN(TOTAL.P1), 'YYYYMMDD')) AS earliest_trn_month,
    COUNT(*) AS total_customers,
    SUM(CASE WHEN TOTAL.P15 = 'VIP Customer' THEN 1 ELSE 0 END) AS total_vip_customer_count,
    ROUND(SUM(CASE WHEN TOTAL.P15 = 'VIP Customer' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS total_vip_customer_pct,
    SUM(CASE WHEN TOTAL.P15 = 'Loyal Customer' THEN 1 ELSE 0 END) AS total_loyal_customer_count,
    ROUND(SUM(CASE WHEN TOTAL.P15 = 'Loyal Customer' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS total_loyal_customer_pct,
    SUM(CASE WHEN TOTAL.P15 = 'One-time Buyer' THEN 1 ELSE 0 END) AS total_one_time_customer_count,
    ROUND(SUM(CASE WHEN TOTAL.P15 = 'One-time Buyer' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS total_one_time_customer_pct,
    LAST_ONE_YEAR.churn_risk_customer_count AS total_churn_risk_customer_count, 
     -- Churned Customers
    (SUM(CASE WHEN TOTAL.P15 = 'at Risk' THEN 1 ELSE 0 END) - LAST_ONE_YEAR.churn_risk_customer_count) AS total_churned_customer_count,
    
    
    -- Last Year Metrics
    LAST_ONE_YEAR.customer_count AS last_year_customer_count,
    LAST_ONE_YEAR.median_recency,
    LAST_ONE_YEAR.median_frequency,
    LAST_ONE_YEAR.median_monetary_value,
    LAST_ONE_YEAR.median_customer_lifespan_value,
    LAST_ONE_YEAR.median_avg_purchase_value,
    LAST_ONE_YEAR.median_clv_value,
    LAST_ONE_YEAR.new_customer_count,
    LAST_ONE_YEAR.new_customer_pct,
    LAST_ONE_YEAR.active_customer_count,
    LAST_ONE_YEAR.active_customer_pct,
    LAST_ONE_YEAR.one_time_customer_count,
    LAST_ONE_YEAR.one_time_customer_pct,
    LAST_ONE_YEAR.vip_customer_count,
    LAST_ONE_YEAR.vip_customer_pct,
    LAST_ONE_YEAR.loyal_customer_count,
    LAST_ONE_YEAR.loyal_customer_pct,
    LAST_ONE_YEAR.potential_growth_customer_count,
    LAST_ONE_YEAR.potential_growth_customer_pct,
    LAST_ONE_YEAR.at_risk_customer_count,
    LAST_ONE_YEAR.at_risk_customer_pct,
    LAST_ONE_YEAR.churn_risk_customer_count,
    LAST_ONE_YEAR.churn_risk_customer_pct,

    -- New Derived Metrics
    -- One-Time to Active Conversion Rate
    ROUND((LAST_ONE_YEAR.active_customer_count - LAST_ONE_YEAR.one_time_customer_count) * 100.0 / NULLIF(LAST_ONE_YEAR.active_customer_count, 0), 2) AS one_time_to_active_conversion_rate,

    -- Retention Ratio
    ROUND(LAST_ONE_YEAR.active_customer_count * 100.0 / NULLIF(LAST_ONE_YEAR.customer_count, 0), 2) AS retention_ratio,

    -- New Customer Contribution
    ROUND(LAST_ONE_YEAR.new_customer_count * 100.0 / NULLIF(LAST_ONE_YEAR.active_customer_count, 0), 2) AS new_customer_contribution_pct,

    -- Lost Revenue from Churn Risk Customers
    ROUND(LAST_ONE_YEAR.churn_risk_customer_count * LAST_ONE_YEAR.median_avg_purchase_value, 2) AS lost_revenue_from_churn_risk,

    -- Projected Revenue from Active Customers
    ROUND(LAST_ONE_YEAR.active_customer_count * LAST_ONE_YEAR.median_avg_purchase_value, 2) AS projected_revenue_from_active_customers
    

FROM 
    {SCHEMA_NAME}.ANALYTIC_CUSTOMER TOTAL

-- Subquery: Customers in Last 1 Year
LEFT JOIN (
    SELECT 
        FIRM_ID,
        COUNT(*) AS customer_count,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY TO_NUMBER(P3)) AS median_recency,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY TO_NUMBER(P4)) AS median_frequency,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY TO_NUMBER(P5)) AS median_monetary_value,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY TO_NUMBER(P11)) AS median_customer_lifespan_value,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY TO_NUMBER(P12)) AS median_avg_purchase_value,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY TO_NUMBER(P14)) AS median_clv_value,
        SUM(CASE WHEN P6 = 'New Customer' THEN 1 ELSE 0 END) AS new_customer_count,
        ROUND(SUM(CASE WHEN P6 = 'New Customer' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS new_customer_pct,
        SUM(CASE WHEN P6 = 'Active Customer' THEN 1 ELSE 0 END) AS active_customer_count,
        ROUND(SUM(CASE WHEN P6 = 'Active Customer' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS active_customer_pct,
        SUM(CASE WHEN P15 = 'One-time Buyer' THEN 1 ELSE 0 END) AS one_time_customer_count,
        ROUND(SUM(CASE WHEN P15 = 'One-time Buyer' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS one_time_customer_pct,
        SUM(CASE WHEN P15 = 'VIP Customer' THEN 1 ELSE 0 END) AS vip_customer_count,
        ROUND(SUM(CASE WHEN P15 = 'VIP Customer' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS vip_customer_pct,
        SUM(CASE WHEN P15 = 'Loyal Customer' THEN 1 ELSE 0 END) AS loyal_customer_count,
        ROUND(SUM(CASE WHEN P15 = 'Loyal Customer' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS loyal_customer_pct,
        SUM(CASE WHEN P15 = 'Potential Growth Customer' THEN 1 ELSE 0 END) AS potential_growth_customer_count,
        ROUND(SUM(CASE WHEN P15 = 'Potential Growth Customer' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS potential_growth_customer_pct,
        SUM(CASE WHEN P6 = 'Active - at risk' THEN 1 ELSE 0 END) AS at_risk_customer_count,
        ROUND(SUM(CASE WHEN P6 = 'Active - at risk' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS at_risk_customer_pct,
        SUM(CASE WHEN P15 = 'at Risk' THEN 1 ELSE 0 END) AS churn_risk_customer_count,
        ROUND(SUM(CASE WHEN P15 = 'at Risk' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS churn_risk_customer_pct

    FROM 
        {SCHEMA_NAME}.ANALYTIC_CUSTOMER
    WHERE 
        TO_DATE(P2, 'YYYYMMDD') >= ADD_MONTHS(SYSDATE, -12)
    GROUP BY 
        FIRM_ID
) LAST_ONE_YEAR
ON TOTAL.FIRM_ID = LAST_ONE_YEAR.FIRM_ID

GROUP BY 
    TOTAL.FIRM_ID,
    LAST_ONE_YEAR.customer_count,
    LAST_ONE_YEAR.median_recency,
    LAST_ONE_YEAR.median_frequency,
    LAST_ONE_YEAR.median_monetary_value,
    LAST_ONE_YEAR.median_customer_lifespan_value,
    LAST_ONE_YEAR.median_avg_purchase_value,
    LAST_ONE_YEAR.median_clv_value,
    LAST_ONE_YEAR.new_customer_count,
    LAST_ONE_YEAR.new_customer_pct,
    LAST_ONE_YEAR.active_customer_count,
    LAST_ONE_YEAR.active_customer_pct,
    LAST_ONE_YEAR.one_time_customer_count,
    LAST_ONE_YEAR.one_time_customer_pct,
    LAST_ONE_YEAR.vip_customer_count,
    LAST_ONE_YEAR.vip_customer_pct,
    LAST_ONE_YEAR.loyal_customer_count,
    LAST_ONE_YEAR.loyal_customer_pct,
    LAST_ONE_YEAR.potential_growth_customer_count,
    LAST_ONE_YEAR.potential_growth_customer_pct,
    LAST_ONE_YEAR.at_risk_customer_count,
    LAST_ONE_YEAR.at_risk_customer_pct,
    LAST_ONE_YEAR.churn_risk_customer_count,
    LAST_ONE_YEAR.churn_risk_customer_pct
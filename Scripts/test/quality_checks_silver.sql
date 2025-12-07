/*
===============================================================================
Bronze Layer – Data Quality Validation
===============================================================================
Script Purpose:
    This script performs foundational data quality checks against the 
    `Bronze.crm_customer` source data to ensure readiness for transformation 
    into the Silver layer. The validations focus on:
        - Identification of duplicate business keys.
        - Completeness of customer lifecycle information.
        - Detection of leading/trailing whitespaces in text columns.

Scope:
    These checks are designed to profile raw ingestion quality and expose
    anomalies that may affect downstream modelling of customer behaviour,
    segmentation, and lifecycle analytics.

Usage Notes:
    - Execute after initial Bronze ingestion.
    - Investigate and remediate any results returned.
    - Zero rows returned indicates expected data quality.
===============================================================================
*/

-- ====================================================================
-- Check for duplicate Customer IDs (business key)
-- ====================================================================
SELECT customer_id,
       COUNT(*) AS nos_customers   
FROM Bronze.crm_customer
GROUP BY customer_id
HAVING COUNT(*) > 1;
GO

-- ====================================================================
-- Check for missing lifetime expectation values
-- ====================================================================
SELECT [status], lifetime_months_expected
FROM Bronze.crm_customer
WHERE lifetime_months_expected IS NULL;
GO

-- ====================================================================
-- Check for leading/trailing spaces in acquisition channel
-- ====================================================================
SELECT acquisition_channel
FROM Bronze.crm_customer
WHERE acquisition_channel <> LTRIM(RTRIM(acquisition_channel));
GO


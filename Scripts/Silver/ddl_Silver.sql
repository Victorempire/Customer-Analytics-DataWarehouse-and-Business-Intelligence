/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/
IF OBJECT_ID('Silver.crm_customer','U') IS NOT NULL DROP TABLE Silver.crm_customer;
CREATE TABLE Silver.crm_customer (
    customer_id NVARCHAR(100) PRIMARY KEY,
    signup_date DATETIME NULL,
    customer_segment NVARCHAR(100) NULL,
    region NVARCHAR(100) NULL,
    "status" NVARCHAR(100) NULL,
    lifetime_months_expected INT NULL,
    acquisition_channel NVARCHAR(100) NULL,
    acquisition_campaign NVARCHAR(100) NULL,
    acquisition_cost_ngn  INT NULL
);
GO

IF OBJECT_ID('Silver.crm_transaction','U') IS NOT NULL DROP TABLE Silver.crm_transaction;
CREATE TABLE Silver.crm_transaction (
    transaction_id VARCHAR(50) ,
	customer_id VARCHAR(50) NULL,
	transaction_date DATETIME NULL,
    product_category VARCHAR(100) NULL,
    payment_method VARCHAR(50) NULL,
    transaction_amount_ngn INT NULL,
    cost_to_serve_ngn INT NULL,
    is_first_month_purchase INT NULL
);
GO

IF OBJECT_ID('Silver.crm_activities','U') IS NOT NULL DROP TABLE Silver.crm_activities;
CREATE TABLE Silver.crm_activities (
	activity_id VARCHAR(50) ,
    activity_type VARCHAR(50) NULL,
    activity_value VARCHAR(255) NULL,
    channel VARCHAR(100) NULL,
    customer_id VARCHAR(50) NULL,
    activity_date DATE NULL
);
GO

IF OBJECT_ID('Silver.crm_campaign','U') IS NOT NULL DROP TABLE Silver.crm_campaign;
CREATE TABLE Silver.crm_campaign (
  campaign_id VARCHAR(50) PRIMARY KEY,
    campaign_name VARCHAR(255) NULL,
    channel VARCHAR(100) NULL,
    campaign_start_date DATE NULL,
    campaign_end_date DATE NULL,
    budget_ngn DECIMAL(18,2) NULL,
    leads_generated INT NULL,
    new_customers_acquired INT NULL
);
GO

/*
===============================================================================
Silver Layer Load + Standardization (Bronze → Silver)
===============================================================================
Purpose:
    Clean, standardize and transform raw Bronze data into analytics-ready Silver
    tables. The Silver layer removes duplicates, enforces business rules, and
    applies semantic normalization required for reporting and modeling.

Core Transformations Performed:
    • Remove leading/trailing spaces
    • Standardize acquisition channel values
    • Normalize activity channel categories
    • Remove duplicate customer records using ROW_NUMBER()
    • Clean channel codes across Customer, Activities & Campaign datasets
    • Ensure Silver conforms to business naming conventions

Business Rules Applied:
    - 'EVT' → 'Event'
    - 'AFF' → 'Affiliate'
    - 'REF' → 'Referral'
    - Multiple organic/social spellings → 'Organic Social'
    - Multiple paid social spellings → 'Paid Social'

Why this is necessary:
    Marketing data typically contains inconsistent codes across CRM,
    campaigns and activities. Consolidating channel naming makes
    acquisition efficiency, attribution, and ROI calculations possible
    during EDA and BI analytics.

Load Pattern:
    TRUNCATE + INSERT into Silver tables.
    Silver is always rebuilt from cleansed Bronze data.

Notes:
    - Silver tables contain no duplicates
    - Silver is the primary layer used for EDA and analytics
    - Business metrics such as CAC, CLV, ROI rely on these transformations
===============================================================================
*/

TRUNCATE TABLE Silver.crm_customer;
INSERT INTO Silver.crm_customer(
	  [customer_id]
      ,[signup_date]
      ,[customer_segment]
      ,[region]
      ,[status]
      ,[lifetime_months_expected]
      ,[acquisition_channel]
      ,[acquisition_campaign]
      ,[acquisition_cost_ngn])
SELECT
	   [customer_id]
      ,[signup_date]
      ,[customer_segment]
      ,[region]
      ,[status]
      ,[lifetime_months_expected]
      ,[acq_std] AS [acquisition_channel]           ----Remove Unwanted Spaces
      ,[acquisition_campaign]
      ,[acquisition_cost_ngn]
FROM(
		SELECT [customer_id]
			  ,[signup_date]
			  ,[customer_segment]
			  ,[region]
			  ,[status]
			  ,[lifetime_months_expected]
			  ,[acquisition_campaign]
			  ,[acquisition_cost_ngn]
	  ,CASE
            WHEN LTRIM(RTRIM(acquisition_channel)) = 'EVT' THEN 'Event'
            WHEN LTRIM(RTRIM(acquisition_channel)) = 'AFF' THEN 'Affiliate'
            WHEN LTRIM(RTRIM(acquisition_channel)) = 'REF' THEN 'Referral'
			WHEN UPPER(TRIM(acquisition_channel)) IN ('Organic_Social','Social','Organic Socia','ORG','SOC') THEN 'Organic Social'
			WHEN UPPER(TRIM(acquisition_channel)) IN ('Paid_Social','PS') THEN 'Paid Social'
            ELSE LTRIM(RTRIM(acquisition_channel))                                    -----Standardization of acquisition channel
			END AS acq_std	
				,ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY customer_id ) AS flag
		FROM Bronze.crm_customer
) T
WHERE flag=1                                                             -------Remove Duplicate
GO

---Activity Table
TRUNCATE TABLE Silver.crm_activities;
INSERT INTO Silver.crm_activities(
	   [activity_id]
      ,[customer_id]
      ,[activity_date]
      ,[activity_type]
      ,[activity_value]
      ,[channel]
)
SELECT [activity_id]
      ,[customer_id]
      ,[activity_date]
      ,[activity_type]
      ,[activity_value]
      ,[channel]
FROM (
SELECT [activity_id]
      ,[customer_id]
      ,[activity_date]
      ,[activity_type]
      ,[activity_value]
		,CASE
			WHEN UPPER(TRIM(channel))='EVT' THEN 'Event'
			WHEN UPPER(TRIM(channel))='AFF' THEN 'Affiliate'
			WHEN UPPER(TRIM(channel))='REF' THEN 'Referral'
			WHEN UPPER(TRIM(channel)) IN ('Organic_Social','Social','Organic Socia','ORG','SOC')
			THEN 'Organic Social'
			WHEN UPPER(TRIM(channel)) IN ('Paid_Social','PS') THEN 'Paid Social'
			END AS Channel
FROM[Datawarehouse].[Bronze].[crm_activities]
) T
GO

----Transactions Table
INSERT INTO Silver.crm_transaction(
       [transaction_id]
      ,[customer_id]
      ,[transaction_date]
      ,[transaction_amount_ngn]
      ,[product_category]
      ,[payment_method]
      ,[is_first_month_purchase]
      ,[cost_to_serve_ngn]
)
SELECT [transaction_id]
      ,[customer_id]
      ,[transaction_date]
      ,[transaction_amount_ngn]
      ,[product_category]
      ,[payment_method]
      ,[is_first_month_purchase]
      ,[cost_to_serve_ngn]
FROM Bronze.crm_transaction;
GO

---Campaign Table
TRUNCATE TABLE Silver.crm_campaign;
INSERT INTO Silver.crm_campaign(
	   [campaign_id]
      ,[campaign_name]
      ,[channel]
      ,[campaign_start_date]
      ,[campaign_end_date]
      ,[budget_ngn]
      ,[leads_generated]
      ,[new_customers_acquired]
)
SELECT [campaign_id]
      ,[campaign_name]
	  ,[Channel]
      ,[campaign_start_date]
      ,[campaign_end_date]
      ,[budget_ngn]
      ,[leads_generated]
      ,[new_customers_acquired]
FROM(
SELECT [campaign_id]
      ,[campaign_name]
      ,[campaign_start_date]
      ,[campaign_end_date]
      ,[budget_ngn]
      ,[leads_generated]
      ,[new_customers_acquired]
	  ,CASE
            WHEN LTRIM(RTRIM(channel)) = 'EVT' THEN 'Event'
            WHEN LTRIM(RTRIM(channel)) = 'AFF' THEN 'Affiliate'
            WHEN LTRIM(RTRIM(channel)) = 'REF' THEN 'Referral'
			WHEN UPPER(TRIM(channel)) IN ('Organic_Social','Social','Organic Socia','ORG','SOC') THEN 'Organic Social'
			WHEN UPPER(TRIM(channel)) IN ('Paid_Social','PS') THEN 'Paid Social'
            ELSE LTRIM(RTRIM(channel))  
		END AS  Channel
from Bronze.crm_campaign
) T
GO

/*
===============================================================================
Indexing (Silver Layer)
===============================================================================
Purpose:
    These indexes are created on the Silver Layer tables to support
    analytical workloads, speed up JOIN operations, and optimize query
    performance across commonly filtered and grouped fields.

Rationale:
    - Improve lookup and filtering speed on high-cardinality columns
    - Support fact-to-dimension joins on customer_id
    - Improve analytical queries on acquisition channels and marketing data
    - Reduce scan time during EDA and dashboard reporting

Scope:
    Indexes are applied across:
        • Customer dimension
        • Transaction fact table
        • Activities fact table
        • Campaign data

Index Types:
    Non-clustered indexes were selected since:
        - Silver layer is optimized for analytic reads
        - Base tables remain writable
        - Clustered index is managed automatically by SQL Server

Notes:
    - Review index usage periodically (sys.dm_db_index_usage_stats)
===============================================================================
*/
 ---Create index for Facts and Diamension Table
---Customer Table
CREATE NONCLUSTERED INDEX NIX_crm_customer_channel     
ON Silver.crm_customer(acquisition_channel);

CREATE NONCLUSTERED INDEX NIX_crm_customer_signup
ON Silver.crm_customer(signup_date);

---Transaction Table
CREATE NONCLUSTERED INDEX NIX_transaction_customer
ON Silver.crm_transactions(customer_id);

CREATE NONCLUSTERED INDEX NIX_transaction_date
ON Silver.crm_transactions(transaction_date);

CREATE NONCLUSTERED INDEX NIX_transaction_product
ON Silver.crm_transactions(product_category);

---Activity table
CREATE NONCLUSTERED INDEX NIX_activities_customer
ON Silver.crm_activities(customer_id);

CREATE NONCLUSTERED INDEX NIX_activities_date
ON Silver.crm_activities(activity_date);

CREATE NONCLUSTERED INDEX NIX_activities_type
ON Silver.crm_activities(activity_type);

---Campaign Table
CREATE NONCLUSTERED INDEX NIX_campaign_channel
ON Silver.crm_campaign(channel);

CREATE NONCLUSTERED INDEX NIX_campaign_start
ON Silver.crm_campaign(campaign_start_date);


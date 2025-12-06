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


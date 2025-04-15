create view gold.dim_customers as
SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_first_name AS first_name,
    ci.cst_last_name AS last_name,
    la.cntry AS country,
    CASE 
        WHEN ci.cst_gender != 'n/a' THEN ci.cst_gender  -- CRM is the Master for gender Info
        ELSE COALESCE(ca.gen, 'n/a')
    END AS gender,
    ca.bdate AS birthdate,
    ci.dwh_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid


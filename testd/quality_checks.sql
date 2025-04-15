
--show the data in first table 
select date from bronze.crm_cust_info;

--1.check for null 
select count(*) 
from bronze.crm_cust_info
where cst_id is Null;

--2.check for duplicates
select cst_id,count(*) as 'duplicates'
from bronze.crm_cust_info
group by cst_id
having count(*) > 1;

--check for unwanted spaces

select cst_first_name ,trim(cst_first_name)
from bronze.crm_cust_info
where cst_first_name !=trim(cst_first_name);

select distinct cst_gender
from bronze.crm_cust_info;


select distinct cst_gender
from bronze.crm_cust_info;
----------------------------------------------------silver table-------------------------------------------------------------
--  check duplicate in silver table 

select cst_id,count(*) as 'duplicates'
from silver.crm_cust_info
group by cst_id
having count(*) > 1;

--
select * from silver.crm_cust_info

-----------------------------------------------second table-------------------------
select * from bronze.crm_prd_info

--1.check for null 
select count(*) 
from bronze.crm_prd_info
where prd_id is Null;
--res is 0

--check for duplicate 
select prd_id,count(*) as 'duplicates'
from bronze.crm_prd_info
group by prd_id
having count(*) > 1;


----
select distinct prd_line
from bronze.crm_prd_info;

------
select prd_start_dt
from bronze.crm_prd_info
where ISDATE(prd_start_dt)=0;

--convert data type to date 

alter table bronze.crm_prd_info
alter column prd_start_dt datetime


--------------------crm saes------
select * from bronze.crm_sales_details
---sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_due_dt,sls_quantity,sls_price
--order date
select nullif(sls_order_dt,0) sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <=0 or len(sls_order_dt) != 8 --good date is 8 digits 19022002

---------sales
--check the sales 
select  sls_sales as old_sales,
sls_quantity,
sls_price as old_price,
CASE 
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,
    CASE 
        WHEN sls_price IS NULL OR sls_price <= 0
		then sls_sales /nullif(sls_quantity,0)
		else sls_price
	end as sls_price
from bronze.crm_sales_details
where sls_sales!=sls_quantity*sls_price
or sls_sales is null or sls_price is null or sls_quantity is null

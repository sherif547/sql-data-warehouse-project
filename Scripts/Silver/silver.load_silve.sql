CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	declare @start_time Datetime, @endtime datetime 

	--insert into silver tabe product info
	set @start_time=GETDATE();
	print('loading ..........')

	truncate table silver.crm_prd_info
	insert into silver.crm_prd_info(prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
	select 
	prd_id,
	Replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
	SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
	prd_nm,
	isnull(prd_cost,0) as prd_cost,
	case when upper(prd_line)='M' then 'Mountain'
		when upper(prd_line)='R' then 'Road'
		when upper(prd_line)='S' then 'Other Sale'
		when upper(prd_line)='T' then 'Touring'
		else 'n/a'
	end as prd_line,
	cast(prd_start_dt as date),
	cast((lead(prd_start_dt)over(partition by prd_key order by prd_start_dt)-1) as date) as prd_end_dt 

	from bronze.crm_prd_info
	print('insert into product table Done')
	print('-----------------------------------------')
	--- insert into sales details


	truncate table silver.crm_sales_details
	insert into silver.crm_sales_details (sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_due_dt,sls_ship_dt,sls_sales,sls_quantity,sls_price)
	select
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	-- fix a date 
	 case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null 
		else cast(cast(sls_order_dt as varchar)as date)
	 end as sls_order_dt,
	 case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null 
		else cast(cast(sls_due_dt as varchar)as date)
	 end as sls_due_dt,

	  case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null 
		else cast(cast(sls_ship_dt as varchar)as date)
	 end as sls_ship_dt,


	 CASE 
			WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
	 END AS sls_sales,

	sls_quantity,

	 CASE 
			WHEN sls_price IS NULL OR sls_price <= 0
			then sls_sales /nullif(sls_quantity,0)
			else sls_price
	end as sls_price

	from bronze.crm_sales_details

	print 'sales details Done inserted '
	print '-----------------------------------------------------------'
	--------------------------------------------insert into customer informations---

	-- cahnge data type from string to datetime

	/*update bronze.crm_cust_info
	set date=right(date,10);

	SELECT date
	FROM bronze.crm_cust_info
	WHERE ISDATE(date) = 0;

	DELETE FROM bronze.crm_cust_info
	WHERE ISDATE(date) = 0;


	ALTER TABLE bronze.crm_cust_info
	ALTER COLUMN date DATE;

	-- complete the cleaning Remoce Duplicates
	*/
	truncate table silver.crm_cust_info
	insert into silver.crm_cust_info(cst_id,cst_key,cst_first_name,cst_last_name,cst_gender,Date)

	select 
	cst_id,
	cst_key,
	trim(cst_first_name) as first_name,
	trim(cst_last_name) as last_name,

	case when upper(Trim(cst_gender)) = 'M' then 'Male'
		when upper(Trim(cst_gender)) ='S' then 'Female'
		else 'n/a'
	end cst_gender 
		,
	Date
	from
	(
	select * , ROW_NUMBER()over(partition by cst_id order by date) as flag_last
	from bronze.crm_cust_info
	) t
	where flag_last = 1
	;
	 print 'customer informations inserted '
	 print('----------------------------------------')

	-----------------------------
	print'loading erp........! '
	truncate table silver.erp_px_cat_g1v2
	insert into silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
	select id,cat,subcat,maintenance from
	bronze.erp_px_cat_g1v2
	print 'data inserted into erp px_cat'
	print'---------------------------';

	truncate table silver.erp_loc_a101
	insert into  silver.erp_loc_a101(cid,cntry)
	select replace(cid,'-','') as cid,
	case when trim(upper(cntry)) in ('US','UNITED STATES','USA') then 'US'
		when trim(cntry) ='DE' then 'Germany'
		when cntry='' or cntry is null then 'n/a'
	else cntry
	end as cntry
	from bronze.erp_loc_a101
	where cntry='UNITED STATES'

	print 'data inserted into erp loc'

	-----------------------------------
	truncate table silver.erp_cust_az12
	insert into silver.erp_cust_az12(cid,bdate,gen)
	select 

	case when cid like 'NAS%' then SUBSTRING(cid,4,LEN(cid))
	else cid
	end cid,


	case when bdate > GETDATE() then null
	else bdate
	end as bdate,

	case when trim(upper(gen))in ('MALE','M') then 'Male'
		when  trim(upper(gen)) in ('FEMALE','F') then 'Female'
		else 'N/a'

	end as gen
	from bronze.erp_cust_az12


	print 'data inserted into erp customer'

	set @endtime=GETDATE();

	PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @endtime) AS NVARCHAR) + ' seconds';
end

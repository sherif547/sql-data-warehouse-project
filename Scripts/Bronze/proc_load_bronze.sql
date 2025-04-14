create or Alter procedure bronze.load_bronze  as 
Begin


	--make sure the table is not empty
	truncate table bronze.crm_cust_info;

	Bulk Insert bronze.crm_cust_info
	from 'D:\De\DWH\first _proj\Data\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	with (
		firstRow=2,
		FieldTerminator=',',
		Tablock	
		);

	select * from bronze.crm_cust_info;
	select count(*) from bronze.crm_cust_info;

	-- other tables

	--make sure the table is not empty


	--truncate table bronze.crm_cust_info;


	BULK INSERT bronze.crm_prd_info
	FROM 'D:\De\DWH\first _proj\Data\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		ROWTERMINATOR = '\n',
		TABLOCK
  
	);

	select * from bronze.crm_prd_info;


	bulk insert bronze.crm_sales_details
	from 'D:\De\DWH\first _proj\Data\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	with
	(
		 FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		ROWTERMINATOR = '\n',
		TABLOCK
  

	);




	bulk insert bronze.erp_cust_az12
	from 'D:\De\DWH\first _proj\Data\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
	with
	(
		 FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		ROWTERMINATOR = '\n',
		TABLOCK
  

	)




	bulk insert bronze.erp_loc_a101
	from 'D:\De\DWH\first _proj\Data\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
	with
	(
		 FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		ROWTERMINATOR = '\n',
		TABLOCK
  

	)




	bulk insert bronze.erp_px_cat_g1v2
	from 'D:\De\DWH\first _proj\Data\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
	with
	(
		 FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		ROWTERMINATOR = '\n',
		TABLOCK
  

	)

End

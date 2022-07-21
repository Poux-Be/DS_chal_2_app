-- Instance creations
create or replace database ds_chal_2;
create or replace schema intel_immo;

-- Table Creation
create or replace table sales
(
     transaction_date	    VARCHAR(50)
    ,transaction_nature    	VARCHAR(25)
    ,transaction_value	    NUMBER(10,0)
    ,street_number	        NUMBER(6,0)
    ,street_type_code       NUMBER(6,0)
    ,street_type            VARCHAR(25)
    ,street_code    	    VARCHAR(25)
    ,street_name	        VARCHAR(100)
    ,city_id_code   	    NUMBER(4,0)
    ,zip_code	            NUMBER(6,0)
    ,city_name	            VARCHAR(50)
    ,dept_code      	    VARCHAR(50)
    ,city_code      	    NUMBER(5,0)
    ,section_prefix	        VARCHAR(25)
    ,section        	    VARCHAR(50)
    ,plan_number	        NUMBER(6,0)
    ,volume_number	        NUMBER(6,0)
    ,first_lot  	        VARCHAR(25)
    ,carrez_surface	        FLOAT
    ,lot_number 	        NUMBER(4,0)
    ,local_type_code        VARCHAR(25)
    ,local_type     	    VARCHAR(25)
    ,local_id	            VARCHAR(25)
    ,actual_surface	        NUMBER(6,0)
    ,rooms_number	        NUMBER(6,0)
    ,culture_nature	        VARCHAR(25)
    ,spe_culture_nature	    VARCHAR(25)
    ,terrain_surface	    NUMBER(10,0)
  );
  
  -- Add a new table for departments informations
create or replace table dept_info
(
     lat	    FLOAT
    ,lon    	FLOAT
    ,insee_code	VARCHAR(50)
    ,name       VARCHAR(50)
    ,old_region VARCHAR(50)
    ,new_region VARCHAR(50)
);
  
  -- File format to load data
  create or replace file format ds_chal_2.intel_immo.sales_data_load
  COMPRESSION = 'AUTO'
  FIELD_DELIMITER = ','
  RECORD_DELIMITER = '\n'
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  TRIM_SPACE = FALSE
  ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
  ESCAPE = 'NONE'
  ESCAPE_UNENCLOSED_FIELD = '\134'
  DATE_FORMAT = 'DD/MM/YYYYY HH:MM:SS' 
  TIMESTAMP_FORMAT = 'DD/MM/YYYYY HH:MM:SS' 
  NULL_IF = ('nan');
  
  -----------------------------
  -- Data to be loaded manually
  -----------------------------
  
  -- Print loaded data
  select * from sales limit 20;
  
  --
  select COLUMN_NAME from information_schema.columns where table_name = 'SALES';
  
  -----------------------------
  -- Create a secure view to work on
  -----------------------------
  
  create or replace view sales_view as
    select
         to_date(transaction_date) as transaction_date
        ,transaction_nature
        ,transaction_value
        ,street_number
        ,street_type_code
        ,street_type
        ,street_code
        ,street_name
        ,city_id_code
        ,zip_code
        ,city_name
        ,dept_code
        ,city_code
        ,section_prefix
        ,section
        ,plan_number
        ,volume_number
        ,first_lot
        ,carrez_surface
        ,lot_number
        ,local_type_code
        ,local_type
        ,local_id
        ,actual_surface
        ,rooms_number
        ,culture_nature
        ,spe_culture_nature
        ,terrain_surface
    from sales
    ;
    
select dept_code, avg(transaction_value/carrez_surface) as avg_sqm_price from sales_view group by dept_code order by avg_sqm_price desc;

-- Update department codes to ensure they match
update sales set dept_code = case when len(dept_code) = 1 then concat('0',dept_code) else dept_code end;
update dept_info set insee_code = case when len(insee_code) = 1 then concat('0',insee_code) else insee_code end;

select distinct dept_code from sales_view order by dept_code;
select insee_code,new_region from dept_info;
select insee_code from dept_info where new_region="Provence-Alpes-Côte-d\'Azur";

select transaction_value, street_number, street_type, city_name, dept_code, carrez_surface, rooms_number from sales_view where (transaction_value is not null and local_type='Appartement') order by transaction_value desc limit 10;

select count(*) from sales_view where (transaction_date>='2020-01-01' and transaction_date <='2020-03-31');

select dept_code, date_part(quarter,transaction_date::date) as t_quarter, sum(count(*)) over (partition by dept_code, t_quarter) as sales_count from sales_view group by dept_code, t_quarter;

select city_name, round(avg(transaction_value)) as avg_price from sales_view where dept_code in ('06', '13', '33', '59', '69') group by city_name order by avg_price desc limit 3;
                            
                            
                            
                            
                            
                            
                            
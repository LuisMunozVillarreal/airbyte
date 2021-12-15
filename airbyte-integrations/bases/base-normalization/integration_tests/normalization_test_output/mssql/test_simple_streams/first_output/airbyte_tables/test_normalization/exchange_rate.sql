
   
  USE [test_normalization];
  if object_id ('test_normalization."exchange_rate__dbt_tmp_temp_view"','V') is not null
      begin
      drop view test_normalization."exchange_rate__dbt_tmp_temp_view"
      end


   
   
  USE [test_normalization];
  if object_id ('test_normalization."exchange_rate__dbt_tmp"','U') is not null
      begin
      drop table test_normalization."exchange_rate__dbt_tmp"
      end


   USE [test_normalization];
   EXEC('create view test_normalization."exchange_rate__dbt_tmp_temp_view" as
    
with __dbt__cte__exchange_rate_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "test_normalization".test_normalization._airbyte_raw_exchange_rate
select
    json_value(_airbyte_data, ''$."id"'') as id,
    json_value(_airbyte_data, ''$."currency"'') as currency,
    json_value(_airbyte_data, ''$."date"'') as "date",
    json_value(_airbyte_data, ''$."timestamp_col"'') as timestamp_col,
    json_value(_airbyte_data, ''$."HKD@spéçiäl & characters"'') as "HKD@spéçiäl & characters",
    json_value(_airbyte_data, ''$."HKD_special___characters"'') as hkd_special___characters,
    json_value(_airbyte_data, ''$."NZD"'') as nzd,
    json_value(_airbyte_data, ''$."USD"'') as usd,
    json_value(_airbyte_data, ''$."column`_''''with\"_quotes"'') as "column`_''with""_quotes",
    _airbyte_ab_id,
    _airbyte_emitted_at,
    SYSDATETIME() as _airbyte_normalized_at
from "test_normalization".test_normalization._airbyte_raw_exchange_rate as table_alias
-- exchange_rate
where 1 = 1
),  __dbt__cte__exchange_rate_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__exchange_rate_ab1
select
    cast(id as 
    bigint
) as id,
    cast(currency as 
    VARCHAR(max)) as currency,
    try_parse(nullif("date", '''') as date) as "date",
    try_parse(nullif(timestamp_col, '''') as datetime) as timestamp_col,
    cast("HKD@spéçiäl & characters" as 
    float
) as "HKD@spéçiäl & characters",
    cast(hkd_special___characters as 
    VARCHAR(max)) as hkd_special___characters,
    cast(nzd as 
    float
) as nzd,
    cast(usd as 
    float
) as usd,
    cast("column`_''with""_quotes" as 
    VARCHAR(max)) as "column`_''with""_quotes",
    _airbyte_ab_id,
    _airbyte_emitted_at,
    SYSDATETIME() as _airbyte_normalized_at
from __dbt__cte__exchange_rate_ab1
-- exchange_rate
where 1 = 1
),  __dbt__cte__exchange_rate_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__exchange_rate_ab2
select
    convert(varchar(32), HashBytes(''md5'',  coalesce(cast(
    
    

    concat(concat(coalesce(cast(id as 
    VARCHAR(max)), ''''), ''-'', coalesce(cast(currency as 
    VARCHAR(max)), ''''), ''-'', coalesce(cast("date" as 
    VARCHAR(max)), ''''), ''-'', coalesce(cast(timestamp_col as 
    VARCHAR(max)), ''''), ''-'', coalesce(cast("HKD@spéçiäl & characters" as 
    VARCHAR(max)), ''''), ''-'', coalesce(cast(hkd_special___characters as 
    VARCHAR(max)), ''''), ''-'', coalesce(cast(nzd as 
    VARCHAR(max)), ''''), ''-'', coalesce(cast(usd as 
    VARCHAR(max)), ''''), ''-'', coalesce(cast("column`_''with""_quotes" as 
    VARCHAR(max)), ''''),''''), '''') as 
    VARCHAR(max)), '''')), 2) as _airbyte_exchange_rate_hashid,
    tmp.*
from __dbt__cte__exchange_rate_ab2 tmp
-- exchange_rate
where 1 = 1
)-- Final base SQL model
-- depends_on: __dbt__cte__exchange_rate_ab3
select
    id,
    currency,
    "date",
    timestamp_col,
    "HKD@spéçiäl & characters",
    hkd_special___characters,
    nzd,
    usd,
    "column`_''with""_quotes",
    _airbyte_ab_id,
    _airbyte_emitted_at,
    SYSDATETIME() as _airbyte_normalized_at,
    _airbyte_exchange_rate_hashid
from __dbt__cte__exchange_rate_ab3
-- exchange_rate from "test_normalization".test_normalization._airbyte_raw_exchange_rate
where 1 = 1
    ');

   SELECT * INTO "test_normalization".test_normalization."exchange_rate__dbt_tmp" FROM
    "test_normalization".test_normalization."exchange_rate__dbt_tmp_temp_view"

   
   
  USE [test_normalization];
  if object_id ('test_normalization."exchange_rate__dbt_tmp_temp_view"','V') is not null
      begin
      drop view test_normalization."exchange_rate__dbt_tmp_temp_view"
      end

    
   use [test_normalization];
  if EXISTS (
        SELECT * FROM
        sys.indexes WHERE name = 'test_normalization_exchange_rate__dbt_tmp_cci'
        AND object_id=object_id('test_normalization_exchange_rate__dbt_tmp')
    )
  DROP index test_normalization.exchange_rate__dbt_tmp.test_normalization_exchange_rate__dbt_tmp_cci
  CREATE CLUSTERED COLUMNSTORE INDEX test_normalization_exchange_rate__dbt_tmp_cci
    ON test_normalization.exchange_rate__dbt_tmp

   


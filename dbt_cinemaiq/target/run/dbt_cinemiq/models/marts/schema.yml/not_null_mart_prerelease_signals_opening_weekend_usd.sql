select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select opening_weekend_usd
from CINEMAIQ.DEV_marts.mart_prerelease_signals
where opening_weekend_usd is null



      
    ) dbt_internal_test
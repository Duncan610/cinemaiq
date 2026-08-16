select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select trend_date
from CINEMAIQ.DEV_staging.stg_google_trends
where trend_date is null



      
    ) dbt_internal_test
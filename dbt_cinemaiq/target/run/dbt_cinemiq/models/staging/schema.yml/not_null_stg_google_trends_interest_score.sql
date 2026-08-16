select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select interest_score
from CINEMAIQ.DEV_staging.stg_google_trends
where interest_score is null



      
    ) dbt_internal_test
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select KEYWORD
from CINEMAIQ.RAW.raw_google_trends
where KEYWORD is null



      
    ) dbt_internal_test
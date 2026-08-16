select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select headline
from CINEMAIQ.DEV_staging.stg_news_articles
where headline is null



      
    ) dbt_internal_test
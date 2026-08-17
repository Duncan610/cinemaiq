select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select total_articles
from CINEMAIQ.DEV_intermediate.int_news_coverage
where total_articles is null



      
    ) dbt_internal_test
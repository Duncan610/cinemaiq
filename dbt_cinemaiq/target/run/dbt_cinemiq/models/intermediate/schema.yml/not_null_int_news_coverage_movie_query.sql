select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select movie_query
from CINEMAIQ.DEV_intermediate.int_news_coverage
where movie_query is null



      
    ) dbt_internal_test
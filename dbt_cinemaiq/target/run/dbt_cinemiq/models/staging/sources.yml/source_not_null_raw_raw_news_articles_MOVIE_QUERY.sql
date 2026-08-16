select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select MOVIE_QUERY
from CINEMAIQ.RAW.raw_news_articles
where MOVIE_QUERY is null



      
    ) dbt_internal_test
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    movie_query as unique_field,
    count(*) as n_records

from CINEMAIQ.DEV_intermediate.int_news_coverage
where movie_query is not null
group by movie_query
having count(*) > 1



      
    ) dbt_internal_test
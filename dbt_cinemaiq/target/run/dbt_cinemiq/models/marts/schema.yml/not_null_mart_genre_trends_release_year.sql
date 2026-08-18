select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select release_year
from CINEMAIQ.DEV_marts.mart_genre_trends
where release_year is null



      
    ) dbt_internal_test
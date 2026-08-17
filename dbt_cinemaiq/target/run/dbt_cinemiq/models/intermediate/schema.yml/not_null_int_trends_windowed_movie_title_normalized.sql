select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select movie_title_normalized
from CINEMAIQ.DEV_intermediate.int_trends_windowed
where movie_title_normalized is null



      
    ) dbt_internal_test
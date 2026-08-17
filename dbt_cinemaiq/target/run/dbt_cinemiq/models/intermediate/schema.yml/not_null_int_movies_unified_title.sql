select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select title
from CINEMAIQ.DEV_intermediate.int_movies_unified
where title is null



      
    ) dbt_internal_test
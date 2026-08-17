select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select actor_name
from CINEMAIQ.DEV_intermediate.int_movie_cast
where actor_name is null



      
    ) dbt_internal_test
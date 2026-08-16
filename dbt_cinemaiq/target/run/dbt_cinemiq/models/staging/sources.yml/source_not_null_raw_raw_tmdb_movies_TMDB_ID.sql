select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select TMDB_ID
from CINEMAIQ.RAW.raw_tmdb_movies
where TMDB_ID is null



      
    ) dbt_internal_test
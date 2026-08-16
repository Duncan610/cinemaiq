select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select IMDB_ID
from CINEMAIQ.RAW.raw_imdb_ratings
where IMDB_ID is null



      
    ) dbt_internal_test
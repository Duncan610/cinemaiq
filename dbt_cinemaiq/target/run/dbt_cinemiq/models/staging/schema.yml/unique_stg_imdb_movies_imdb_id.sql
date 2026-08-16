select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    imdb_id as unique_field,
    count(*) as n_records

from CINEMAIQ.DEV_staging.stg_imdb_movies
where imdb_id is not null
group by imdb_id
having count(*) > 1



      
    ) dbt_internal_test
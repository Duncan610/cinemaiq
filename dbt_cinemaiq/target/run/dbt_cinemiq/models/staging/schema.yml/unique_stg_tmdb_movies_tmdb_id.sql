select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    tmdb_id as unique_field,
    count(*) as n_records

from CINEMAIQ.DEV_staging.stg_tmdb_movies
where tmdb_id is not null
group by tmdb_id
having count(*) > 1



      
    ) dbt_internal_test
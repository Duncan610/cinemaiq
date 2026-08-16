
    
    

select
    tmdb_id as unique_field,
    count(*) as n_records

from CINEMAIQ.DEV_staging.stg_tmdb_movies
where tmdb_id is not null
group by tmdb_id
having count(*) > 1




    
    

select
    imdb_id as unique_field,
    count(*) as n_records

from CINEMAIQ.DEV_staging.stg_imdb_movies
where imdb_id is not null
group by imdb_id
having count(*) > 1




    
    

select
    tmdb_id as unique_field,
    count(*) as n_records

from CINEMAIQ.DEV_marts.mart_success_drivers
where tmdb_id is not null
group by tmdb_id
having count(*) > 1



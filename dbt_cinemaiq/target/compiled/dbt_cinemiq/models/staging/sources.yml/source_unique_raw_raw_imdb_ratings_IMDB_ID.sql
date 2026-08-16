
    
    

select
    IMDB_ID as unique_field,
    count(*) as n_records

from CINEMAIQ.RAW.raw_imdb_ratings
where IMDB_ID is not null
group by IMDB_ID
having count(*) > 1



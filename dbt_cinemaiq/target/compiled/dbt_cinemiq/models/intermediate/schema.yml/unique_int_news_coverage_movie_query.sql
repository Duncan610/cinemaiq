
    
    

select
    movie_query as unique_field,
    count(*) as n_records

from CINEMAIQ.DEV_intermediate.int_news_coverage
where movie_query is not null
group by movie_query
having count(*) > 1



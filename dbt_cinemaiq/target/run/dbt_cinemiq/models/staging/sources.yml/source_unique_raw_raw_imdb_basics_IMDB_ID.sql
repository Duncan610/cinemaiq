select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    IMDB_ID as unique_field,
    count(*) as n_records

from CINEMAIQ.RAW.raw_imdb_basics
where IMDB_ID is not null
group by IMDB_ID
having count(*) > 1



      
    ) dbt_internal_test
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        gap_category as value_field,
        count(*) as n_records

    from CINEMAIQ.DEV_staging.stg_omdb_ratings
    group by gap_category

)

select *
from all_values
where value_field not in (
    'critics_loved_it','critics_preferred','consensus','audiences_preferred','audiences_loved_it'
)



      
    ) dbt_internal_test
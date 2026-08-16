
    
    

with all_values as (

    select
        revenue_tier as value_field,
        count(*) as n_records

    from CINEMAIQ.DEV_staging.stg_box_office
    group by revenue_tier

)

select *
from all_values
where value_field not in (
    'blockbuster','wide_release','limited_release','low_performer','unknown'
)



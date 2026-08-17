
    
    

with all_values as (

    select
        window_type as value_field,
        count(*) as n_records

    from CINEMAIQ.DEV_intermediate.int_trends_windowed
    group by window_type

)

select *
from all_values
where value_field not in (
    '3_month','12_month','other'
)



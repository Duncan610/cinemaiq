
    
    

with all_values as (

    select
        hype_category as value_field,
        count(*) as n_records

    from CINEMAIQ.DEV_marts.mart_hype_vs_performance
    group by hype_category

)

select *
from all_values
where value_field not in (
    'high_hype','moderate_hype','low_hype'
)



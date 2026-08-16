
    
    

with all_values as (

    select
        sentiment_label as value_field,
        count(*) as n_records

    from CINEMAIQ.DEV_staging.stg_news_articles
    group by sentiment_label

)

select *
from all_values
where value_field not in (
    'positive','negative','neutral'
)



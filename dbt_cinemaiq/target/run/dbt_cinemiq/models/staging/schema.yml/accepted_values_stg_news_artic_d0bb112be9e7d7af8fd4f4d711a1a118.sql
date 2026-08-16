select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

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



      
    ) dbt_internal_test
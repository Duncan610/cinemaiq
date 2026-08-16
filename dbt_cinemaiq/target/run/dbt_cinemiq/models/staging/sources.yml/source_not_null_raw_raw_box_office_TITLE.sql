select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select TITLE
from CINEMAIQ.RAW.raw_box_office
where TITLE is null



      
    ) dbt_internal_test
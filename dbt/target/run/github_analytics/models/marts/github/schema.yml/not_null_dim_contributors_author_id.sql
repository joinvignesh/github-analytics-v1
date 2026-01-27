
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select author_id
from GITHUB_ANALYTICS_DB.ANALYTICS_marts.dim_contributors
where author_id is null



  
  
      
    ) dbt_internal_test
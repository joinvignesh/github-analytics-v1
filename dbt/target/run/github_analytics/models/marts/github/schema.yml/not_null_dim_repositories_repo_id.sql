
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select repo_id
from GITHUB_ANALYTICS_DB.ANALYTICS_marts.dim_repositories
where repo_id is null



  
  
      
    ) dbt_internal_test
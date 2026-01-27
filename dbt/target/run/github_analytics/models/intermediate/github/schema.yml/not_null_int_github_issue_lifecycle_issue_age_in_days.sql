
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select issue_age_in_days
from GITHUB_ANALYTICS_DB.ANALYTICS.int_github_issue_lifecycle
where issue_age_in_days is null



  
  
      
    ) dbt_internal_test
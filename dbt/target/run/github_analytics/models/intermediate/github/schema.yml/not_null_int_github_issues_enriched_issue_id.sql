
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select issue_id
from GITHUB_ANALYTICS_DB.ANALYTICS.int_github_issues_enriched
where issue_id is null



  
  
      
    ) dbt_internal_test

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select activity_date
from GITHUB_ANALYTICS_DB.ANALYTICS_marts.fact_repo_daily_metrics
where activity_date is null



  
  
      
    ) dbt_internal_test

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        issue_state as value_field,
        count(*) as n_records

    from GITHUB_ANALYTICS_DB.ANALYTICS_staging.stg_github_issues
    group by issue_state

)

select *
from all_values
where value_field not in (
    'open','closed'
)



  
  
      
    ) dbt_internal_test
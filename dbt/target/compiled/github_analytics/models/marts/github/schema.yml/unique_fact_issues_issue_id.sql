
    
    

select
    issue_id as unique_field,
    count(*) as n_records

from GITHUB_ANALYTICS_DB.ANALYTICS_marts.fact_issues
where issue_id is not null
group by issue_id
having count(*) > 1



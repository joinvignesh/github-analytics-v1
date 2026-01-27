
    
    

select
    repo_id as unique_field,
    count(*) as n_records

from GITHUB_ANALYTICS_DB.ANALYTICS_marts.dim_repositories
where repo_id is not null
group by repo_id
having count(*) > 1



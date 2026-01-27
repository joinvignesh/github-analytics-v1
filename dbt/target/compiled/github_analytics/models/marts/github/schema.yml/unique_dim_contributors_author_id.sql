
    
    

select
    author_id as unique_field,
    count(*) as n_records

from GITHUB_ANALYTICS_DB.ANALYTICS_marts.dim_contributors
where author_id is not null
group by author_id
having count(*) > 1



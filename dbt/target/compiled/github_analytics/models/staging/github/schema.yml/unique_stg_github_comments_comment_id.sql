
    
    

select
    comment_id as unique_field,
    count(*) as n_records

from GITHUB_ANALYTICS_DB.ANALYTICS_staging.stg_github_comments
where comment_id is not null
group by comment_id
having count(*) > 1



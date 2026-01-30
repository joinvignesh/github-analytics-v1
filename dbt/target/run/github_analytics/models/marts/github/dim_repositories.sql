
  
    

create or replace transient table GITHUB_ANALYTICS_DB.ANALYTICS_MARTS.dim_repositories
    
    
    
    as (with repos as (

    select
        repo_id,
        repo_name,
        repo_full_name,
        owner_login,
        owner_type,
        primary_language,
        repo_description,
        
        -- Current Metrics (Snapshot)
        stars_count,
        forks_count,
        open_issues_count,
        
        created_at,
        updated_at

    from GITHUB_ANALYTICS_DB.ANALYTICS_MARTS_staging.stg_github_repositories

)

select * from repos
    )
;


  
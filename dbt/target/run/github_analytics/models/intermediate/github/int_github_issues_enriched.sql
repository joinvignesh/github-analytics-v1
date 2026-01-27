
  create or replace   view GITHUB_ANALYTICS_DB.ANALYTICS.int_github_issues_enriched
  
  
  
  
  as (
    with issues as (

    select *
    from GITHUB_ANALYTICS_DB.ANALYTICS_staging.stg_github_issues

),

repos as (

    select
        repo_name,
        owner_login,
        primary_language,
        repo_full_name -- Useful for URLs/Grouping
    from GITHUB_ANALYTICS_DB.ANALYTICS_staging.stg_github_repositories

),

joined as (

    select
        -- Issue Details
        i.issue_id,
        i.issue_number,
        i.issue_title,
        i.issue_state,
        i.is_pull_request,
        
        -- Author Details
        i.author_login,
        i.author_id,

        -- Repo Context (Joined Fields)
        i.repo_name,
        i.repo_owner,
        r.repo_full_name,
        r.primary_language,

        -- Metrics
        i.comments_count,

        -- Timestamps
        i.created_at,
        i.updated_at,
        i.closed_at,
        i.ingested_at

    from issues i
    left join repos r
        on i.repo_name = r.repo_name
        and i.repo_owner = r.owner_login -- SAFE JOIN: Matches Owner AND Name

)

select * from joined
  );


with daily_activity as (

    select *
    from GITHUB_ANALYTICS_DB.ANALYTICS_MARTS.int_github_repo_activity

)

select
    repo_name,
    repo_owner,
    activity_date,

    -- Daily Aggregations
    issues_created_count,
    issues_closed_count,
    prs_created_count

from daily_activity
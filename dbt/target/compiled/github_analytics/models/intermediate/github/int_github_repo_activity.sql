with issues as (

    select *
    from GITHUB_ANALYTICS_DB.ANALYTICS_staging.stg_github_issues

),

daily_aggregation as (

    select
        repo_name,
        repo_owner,
        date(created_at) as activity_date,

        -- How many issues were opened this day?
        count(*) as issues_created_count,

        -- How many were closed? (Using Snowflake's COUNT_IF for cleaner code)
        count_if(issue_state = 'closed') as issues_closed_count,
        
        -- How many were PRs?
        count_if(is_pull_request = true) as prs_created_count

    from issues
    group by 1, 2, 3

)

select * from daily_aggregation
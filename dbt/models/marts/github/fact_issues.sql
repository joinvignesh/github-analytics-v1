with issue_lifecycle as (

    select *
    from {{ ref('int_github_issue_lifecycle') }}

)

select
    -- Keys
    issue_id,
    repo_full_name,

    -- NEW: Foreign Key to Dim Contributors
    author_id,
    -- NEW: Degenerate Dimension (Name kept in fact for easy analysis without joins)
    author_login,
    
    -- Attributes
    issue_state,
    is_pull_request,
    primary_language,

    -- Lifecycle Status (Business Logic)
    is_open,
    is_closed,

    -- Timestamps
    created_at,
    closed_at,

    -- Calculated Metrics
    days_to_close,
    issue_age_in_days

from issue_lifecycle
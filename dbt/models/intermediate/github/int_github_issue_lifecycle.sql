with enriched_issues as (

    select *
    from {{ ref('int_github_issues_enriched') }}

),

calculated as (

    select
        issue_id,
        repo_full_name, -- Better for reporting than just name
        primary_language,

        -- NEW: Pass through Author details
        author_id,
        author_login,

        issue_state,
        is_pull_request,

        created_at,
        closed_at,

        -- Business Logic: Time to Resolution
        -- DATEDIFF(part, start, end)
        case
            when closed_at is not null
            then datediff('day', created_at, closed_at)
            else null
        end as days_to_close,

        -- Business Logic: Issue Age (Current Age if open, Final Age if closed)
        datediff(
            'day',
            created_at,
            coalesce(closed_at, current_timestamp)
        ) as issue_age_in_days,

        -- Flags
        case when issue_state = 'open' then true else false end as is_open,
        case when issue_state = 'closed' then true else false end as is_closed

    from enriched_issues

)

select * from calculated
with distinct_authors as (

    select distinct
        author_id,
        author_login
    from {{ ref('stg_github_issues') }}
    where author_id is not null

)

select * from distinct_authors
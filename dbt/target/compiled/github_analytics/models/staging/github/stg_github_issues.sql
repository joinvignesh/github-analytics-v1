with source as (

    select * from GITHUB_ANALYTICS_DB.RAW.github_issues

),

renamed as (

    select
        -- IDs
        ID                              as issue_id,
        NUMBER                          as issue_number,
        USER_ID                         as author_id,
        
        -- Repo Metadata
        SOURCE_OWNER                    as repo_owner,
        SOURCE_REPO                     as repo_name,

        -- Properties
        TITLE                           as issue_title,
        BODY                            as issue_body,
        STATE                           as issue_state,
        LOCKED                          as is_locked,
        USER_LOGIN                      as author_login,
        
        -- Logic
        PULL_REQUEST_URL is not null    as is_pull_request,
        
        -- Metrics
        COMMENTS                        as comments_count,
        
        -- Timestamps (Direct selection)
        CREATED_AT                      as created_at,
        UPDATED_AT                      as updated_at,
        CLOSED_AT                       as closed_at,
        INGESTED_AT                     as ingested_at

    from source

),

deduplicated as (

    select *
    from renamed
    -- Keep only the most recently ingested row for each ID
    qualify row_number() over (partition by issue_id order by ingested_at desc) = 1

)

select * from deduplicated
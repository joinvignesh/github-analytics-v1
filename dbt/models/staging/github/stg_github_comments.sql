with source as (

    select * from {{ source('raw', 'github_comments') }}

),

renamed as (

    select
        -- IDs
        ID                              as comment_id,
        USER_ID                         as commenter_id,
        ISSUE_URL                       as issue_url,
        
        -- Properties
        BODY                            as comment_body,
        USER_LOGIN                      as commenter_login,
        AUTHOR_ASSOCIATION              as author_association,
        
        -- Repo Metadata
        SOURCE_OWNER                    as repo_owner,
        SOURCE_REPO                     as repo_name,

        -- Timestamps (Direct selection)
        CREATED_AT                      as created_at,
        UPDATED_AT                      as updated_at,
        INGESTED_AT                     as ingested_at

    from source

),

deduplicated as (

    select *
    from renamed
    qualify row_number() over (partition by comment_id order by ingested_at desc) = 1

)

select * from deduplicated
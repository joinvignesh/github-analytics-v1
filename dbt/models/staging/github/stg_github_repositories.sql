with source as (

    select * from {{ source('raw', 'github_repositories') }}

),

renamed as (

    select
        -- ID & Keys
        ID                              as repo_id,
        OWNER_ID                        as owner_id,
        
        -- Properties
        NAME                            as repo_name,
        FULL_NAME                       as repo_full_name,
        DESCRIPTION                     as repo_description,
        LANGUAGE                        as primary_language,
        PRIVATE                         as is_private,
        FORK                            as is_fork,
        
        -- Owner Info
        OWNER_LOGIN                     as owner_login,
        OWNER_TYPE                      as owner_type,
        
        -- Metrics
        STARGAZERS_COUNT                as stars_count,
        FORKS_COUNT                     as forks_count,
        OPEN_ISSUES_COUNT               as open_issues_count,
        WATCHERS_COUNT                  as watchers_count,

        -- Timestamps (Already timestamps in Snowflake, so we just rename them)
        CREATED_AT                      as created_at,
        UPDATED_AT                      as updated_at,
        PUSHED_AT                       as pushed_at,
        INGESTED_AT                     as ingested_at

    from source

)

select * from renamed
{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
) }}

SELECT 
    DISTINCT "{{ var("table_prefix") }}_employees_directory_stream".id as external_id,
    NOW() as created,
    NOW() as modified,
    md5(
        '{{ var("integration_id") }}' ||
        "{{ var("table_prefix") }}_employees_directory_stream".id ||
        'user' ||
        'bamboohr'
    )  as id,
    'bamboohr' as source,
    '{{ var("integration_id") }}'::uuid  as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_employees_directory_stream._airbyte_data as last_raw_data, 
    '{{ var("timestamp") }}' as sync_timestamp,
    "{{ var("table_prefix") }}_employees_directory_stream".workemail as email,
    "{{ var("table_prefix") }}_employees_directory_stream".firstname as firstname,
    NULL as birth_date,
    "{{ var("table_prefix") }}_employees_directory_stream".lastname as lastname,
    "{{ var("table_prefix") }}_employees_directory_stream".mobilephone as phone_number,
    NULL as language,
    "{{ var("table_prefix") }}_employees_directory_stream".division as legal_entity_name,
    "{{ var("table_prefix") }}_employees_directory_stream".supervisorEmail as manager_email,
    "{{ var("table_prefix") }}_employees_directory_stream".jobtitle as job_title,
    "{{ var("table_prefix") }}_employees_directory_stream".division as team,
    "{{ var("table_prefix") }}_employees_directory_stream".location as country,
    NULL as contract_start_date,
    NULL as contract_end_date,
    "{{ var("table_prefix") }}_employees_directory_stream".pronouns as gender,
    "{{ var("table_prefix") }}_employees_directory_stream".division as business_unit,
    "{{ var("table_prefix") }}_employees_directory_stream".employeeNumber as employee_number,
    "{{ var("table_prefix") }}_employees_directory_stream".employmentHistoryStatus as contract_type,
    NULL as socio_professional_category,
    NULL as state
FROM "{{ var("table_prefix") }}_employees_directory_stream"
LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_employees_directory_stream
    ON "{{ var("table_prefix") }}_employees_directory_stream"._airbyte_ab_id = "_airbyte_raw_{{ var("table_prefix") }}_employees_directory_stream"._airbyte_ab_id

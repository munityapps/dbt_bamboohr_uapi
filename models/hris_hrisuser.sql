{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
) }}

SELECT 
    DISTINCT "{{ var("table_prefix") }}_custom_reports_stream".id as external_id,
    NOW() as created,
    NOW() as modified,
    md5(
        '{{ var("integration_id") }}' ||
        "{{ var("table_prefix") }}_custom_reports_stream".id ||
        'user' ||
        'bamboohr'
    )  as id,
    'bamboohr' as source,
    '{{ var("integration_id") }}'::uuid  as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_custom_reports_stream._airbyte_data as last_raw_data, 
    '{{ var("timestamp") }}' as sync_timestamp,
    "{{ var("table_prefix") }}_custom_reports_stream".workemail as email,
    "{{ var("table_prefix") }}_custom_reports_stream".firstname as firstname,
    "{{ var("table_prefix") }}_custom_reports_stream".dateofbirth as birth_date,
    "{{ var("table_prefix") }}_custom_reports_stream".lastname as lastname,
    "{{ var("table_prefix") }}_custom_reports_stream".mobilephone as phone_number,
    NULL as language,
    "{{ var("table_prefix") }}_custom_reports_stream".division as legal_entity_name,
    "{{ var("table_prefix") }}_custom_reports_stream".supervisorEmail as manager_email,
    "{{ var("table_prefix") }}_custom_reports_stream".jobtitle as job_title,
    "{{ var("table_prefix") }}_custom_reports_stream".division as team,
    "{{ var("table_prefix") }}_custom_reports_stream".country as country,
    "{{ var("table_prefix") }}_custom_reports_stream".hireDate as contract_start_date,
    "{{ var("table_prefix") }}_custom_reports_stream".terminationdate as contract_start_date,
    "{{ var("table_prefix") }}_custom_reports_stream".gender as gender,
    "{{ var("table_prefix") }}_custom_reports_stream".division as business_unit,
    "{{ var("table_prefix") }}_custom_reports_stream".employeenumber as employee_number,
    "{{ var("table_prefix") }}_custom_reports_stream".employmenthistorystatus as contract_type,
    "{{ var("table_prefix") }}_custom_reports_stream".department as socio_professional_category,
    "{{ var("table_prefix") }}_custom_reports_stream".state as state
FROM "{{ var("table_prefix") }}_custom_reports_stream"
LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_custom_reports_stream
    ON "{{ var("table_prefix") }}_custom_reports_stream"._airbyte_ab_id = "_airbyte_raw_{{ var("table_prefix") }}_custom_reports_stream"._airbyte_ab_id

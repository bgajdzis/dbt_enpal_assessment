{{ config(indexes=[{'columns':['change_time','deal_id',]}]) }}
SELECT * FROM {{ source('postgres_public','deal_changes') }}

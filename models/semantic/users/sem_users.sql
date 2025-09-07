SELECT
    id,
    name,
    email
FROM {{ ref('fct_users') }}

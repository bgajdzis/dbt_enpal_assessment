WITH fields AS (SELECT * FROM {{ source('postgres_public','fields') }})

SELECT
    fields.field_key,
    fields.name AS field_name,
    labels.id AS label_id,
    labels.label AS label_name
FROM fields,
    json_to_recordset(fields.field_value_options::json)
        AS labels (id integer, label text)
WHERE fields.field_key = 'stage_id'

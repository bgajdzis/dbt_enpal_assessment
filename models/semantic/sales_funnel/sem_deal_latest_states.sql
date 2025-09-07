WITH
deal_latest_stage AS (
    SELECT DISTINCT ON (deal_id)
        deal_id,
        change_time,
        new_value AS latest_stage
    FROM {{ ref('fct_deal_changes') }}
    WHERE changed_field_key = 'stage_id'
    ORDER BY 1, 2 DESC
),

deal_lost AS (
    SELECT DISTINCT ON (deal_id)
        deal_id,
        change_time,
        new_value AS lost_reason
    FROM {{ ref('fct_deal_changes') }}
    WHERE changed_field_key = 'lost_reason'
    ORDER BY 1, 2 DESC

),

deal_owner AS (
    SELECT DISTINCT ON (deal_id)
        deal_id,
        change_time,
        new_value AS deal_owner
    FROM {{ ref('fct_deal_changes') }}
    WHERE changed_field_key = 'user_id'
    ORDER BY 1, 2 DESC
)

SELECT DISTINCT
    deal_latest_stage.deal_id,
    dim_fields_stage.label_name AS latest_stage,
    deal_latest_stage.change_time AS stage_transition_timestamp,
    deal_lost.lost_reason IS NOT NULL AS is_deal_lost,
    deal_lost.change_time AS deal_loss_timestamp,
    dim_fields_lost_reason.label_name AS loss_reason
FROM deal_latest_stage
INNER JOIN deal_owner ON deal_latest_stage.deal_id = deal_owner.deal_id
LEFT JOIN deal_lost ON deal_latest_stage.deal_id = deal_lost.deal_id
LEFT JOIN {{ ref('dim_fields_lost_reason') }} ON deal_lost.lost_reason::integer = dim_fields_lost_reason.label_id
LEFT JOIN {{ ref('dim_fields_stage') }} ON deal_latest_stage.latest_stage::integer = dim_fields_stage.label_id

WITH stages_and_activities AS (
    SELECT DISTINCT
        deal_id,
        due_to AS change_time,
        CASE type WHEN 'meeting' THEN 2.1 WHEN 'sc_2' THEN 3.1 END AS stage_id
    FROM {{ ref('fct_activity') }}
    WHERE done AND type IN ('meeting', 'sc_2')
    UNION ALL
    SELECT DISTINCT
        deal_id,
        change_time,
        new_value::numeric AS stage_id
    FROM {{ ref('fct_deal_changes') }}
    WHERE changed_field_key = 'stage_id'
    UNION ALL
    SELECT DISTINCT
        deal_id,
        change_time,
        999 AS stage_id
    FROM {{ ref('fct_deal_changes') }}
    WHERE changed_field_key = 'lost_reason'
)

SELECT
    deal_id,
    stage_id,
    LEAST(change_time, LEAD(change_time) OVER (PARTITION BY deal_id ORDER BY stage_id ASC)) AS stage_entered,
    LEAD(change_time) OVER (PARTITION BY deal_id ORDER BY stage_id ASC) AS stage_exited
FROM stages_and_activities
WHERE stage_id < 999

{% set beginning_of_time = '2024-01-01' %}
WITH reporting_intervals AS (
    SELECT * FROM
        GENERATE_SERIES(timestamp '{{ beginning_of_time }}', LOCALTIMESTAMP, interval '1 MONTH') AS reporting_month
),

aggregated AS (
    SELECT
        reporting_intervals.reporting_month,
        stage_time_ranges.stage_id,
        COUNT(DISTINCT stage_time_ranges.deal_id) AS deals_count
    FROM reporting_intervals
    INNER JOIN
        {{ ref('sem_funnel_stage_times') }} AS stage_time_ranges
        ON
            (stage_time_ranges.stage_entered, stage_time_ranges.stage_exited) OVERLAPS (
                reporting_intervals.reporting_month, reporting_intervals.reporting_month + interval '1 MONTH'
            )
    GROUP BY
        reporting_intervals.reporting_month,
        stage_time_ranges.stage_id
)

SELECT
    aggregated.reporting_month::date AS {{ adapter.quote('month') }},
    CASE aggregated.stage_id WHEN 2.1 THEN 'Sales Call 1' WHEN 3.1 THEN 'Sales Call 2' ELSE fields_stage.label_name END
        AS kpi_name,
    aggregated.stage_id AS funnel_step,
    aggregated.deals_count
FROM aggregated LEFT JOIN {{ ref('dim_fields_stage') }} AS fields_stage ON aggregated.stage_id = fields_stage.label_id
ORDER BY aggregated.reporting_month, aggregated.stage_id

-- Bit of premature optimization, but in a real-world scenario we'll want event stream data to be incremental
-- Likely, the CDC compaction logic will need to be expressed as a custom materialization macro and reused
{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'deal_id',
        on_schema_change='append_new_columns',
        full_refresh= false
    )
}}

WITH recent_changes AS (

    SELECT
        deal_transition_timestamps.deal_id,
        deal_transition_timestamps.lead_generation,
        deal_transition_timestamps.to_qualified_lead,
        deal_transition_timestamps.to_assessment,
        deal_transition_timestamps.to_quote_preparation,
        deal_transition_timestamps.to_negotiation,
        deal_transition_timestamps.to_closing,
        deal_transition_timestamps.to_onboarding,
        deal_transition_timestamps.to_follow_up,
        deal_transition_timestamps.to_renewal

    FROM
        CROSSTAB(
            'SELECT deal_id, new_value, change_time '
            || 'FROM {{ ref('fct_deal_changes') }} '
            || 'WHERE changed_field_key = ''stage_id'' '
            {% if is_incremental() %}
            -- Process data from the last day only. Assuming the DB and the CRM are configured to the same TZ.
                || 'AND (change_time > LOCALTIMESTAMP - interval ''1 DAY'') '
            {% endif %}
            || 'ORDER BY deal_id, change_time'
            ,
            'SELECT stage_no FROM GENERATE_SERIES(1,9) stage_no'
        )
            AS deal_transition_timestamps (
                deal_id INT,
                lead_generation TIMESTAMP,
                to_qualified_lead TIMESTAMP,
                to_assessment TIMESTAMP,
                to_quote_preparation TIMESTAMP,
                to_negotiation TIMESTAMP,
                to_closing TIMESTAMP,
                to_onboarding TIMESTAMP,
                to_follow_up TIMESTAMP,
                to_renewal TIMESTAMP
            )
)

SELECT
{% if not is_incremental() %}
    recent_changes.deal_id,
    recent_changes.lead_generation, 
    recent_changes.to_qualified_lead, 
    recent_changes.to_assessment, 
    recent_changes.to_quote_preparation, 
    recent_changes.to_negotiation, 
    recent_changes.to_closing, 
    recent_changes.to_onboarding, 
    recent_changes.to_follow_up, 
    recent_changes.to_renewal

FROM recent_changes
{% else %}
    recent_changes.deal_id,
    COALESCE(recent_changes.lead_generation, existing_values.lead_generation) AS lead_generation,
    COALESCE(recent_changes.to_qualified_lead, existing_values.to_qualified_lead) AS to_qualified_lead,
    COALESCE(recent_changes.to_assessment, existing_values.to_assessment) AS to_assessment,
    COALESCE(recent_changes.to_quote_preparation, existing_values.to_quote_preparation) AS to_quote_preparation,
    COALESCE(recent_changes.to_negotiation, existing_values.to_negotiation) AS to_negotiation,
    COALESCE(recent_changes.to_closing, existing_values.to_closing) AS to_closing,
    COALESCE(recent_changes.to_onboarding, existing_values.to_onboarding) AS to_onboarding,
    COALESCE(recent_changes.to_follow_up, existing_values.to_follow_up) AS to_follow_up,
    COALESCE(recent_changes.to_renewal, existing_values.to_renewal) AS to_renewal

FROM recent_changes LEFT JOIN {{ this }} AS existing_values ON recent_changes.deal_id = existing_values.deal_id
{% endif %}

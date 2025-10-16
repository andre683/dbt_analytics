{{ config(
    materialized="incremental",
    incremental_strategy="insert_overwrite",
    partition_by={"field": "event_hour", "data_type": "datetime"},
    cluster_by=["event_hour","user_pseudo_id"]
) }}

{% set tz = "America/New_York" %}
{% set run_day = "DATE_SUB(CURRENT_DATE('" ~ tz ~ "'), INTERVAL 1 DAY)" %}
{% set suffix  = "FORMAT_DATE('%Y%m%d', " ~ run_day ~ ")" %}

{% set brands = [
  {"brand": "law", "src": "ga4_law"},
  {"brand": "wsj", "src": "ga4_wsj"},
  {"brand": "tcm", "src": "ga4_tcm"},
  {"brand": "npr", "src": "ga4_npr"},
  {"brand": "ngo", "src": "ga4_ngo"},
  {"brand": "osw", "src": "ga4_osw"},
  {"brand": "fox", "src": "ga4_fox"}
] %}

WITH base AS (
  {% for b in brands %}
  SELECT
    DATETIME_TRUNC(DATETIME(TIMESTAMP_MICROS(event_timestamp), {{ tz|tojson }}), HOUR) AS event_hour,
    user_pseudo_id,
    CONCAT(
      user_pseudo_id,
      CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    ) AS session_id,
    COALESCE(
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'transaction_id'),
      CAST(ecommerce.transaction_id AS STRING)
    ) AS transaction_id,
    ecommerce.purchase_revenue,
    '{{ b.brand }}' AS brand
  FROM {{ source(b.src, 'events_intraday_') }}
  WHERE _TABLE_SUFFIX = {{ suffix }}
  {% if not loop.last %}UNION ALL{% endif %}
  {% endfor %}
)

SELECT * FROM base


{{ config(
  persist_docs={"relation": true, "columns": true}
) }}

WITH raw_data AS (

    SELECT
        _id,
        ingested_date,
        response,
        status_code,
        channel_name
    FROM
        {{ source('yt_profile_stats', 'yt_profile_stats') }}
    WHERE
        ingested_date > '2024-10-14'
)

SELECT
    response, 
    JSON_EXTRACT_SCALAR(response, '$.items[0].snippet.title') AS channel_title, 
    JSON_EXTRACT_SCALAR(response, '$.items[0].snippet.description') AS channel_description 
FROM
    raw_data

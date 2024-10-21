
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
    _id AS ingestion_id,
    channel_name,
    JSON_EXTRACT_SCALAR(response, '$.items[0].id') AS channel_id,

    JSON_EXTRACT_SCALAR(response, '$.items[0].snippet.title') AS channel_title,
    JSON_EXTRACT_SCALAR(response, '$.items[0].snippet.description') AS channel_description,
    JSON_EXTRACT_SCALAR(response, '$.items[0].snippet.publishedAt') AS channel_created_at,
    JSON_EXTRACT_SCALAR(response, '$.items[0].snippet.country') AS country_code,

    JSON_EXTRACT_SCALAR(response, '$.items[0].statistics.viewCount') AS viewCount,
    JSON_EXTRACT_SCALAR(response, '$.items[0].statistics.subscriberCount') AS subscriberCount,
    JSON_EXTRACT_SCALAR(response, '$.items[0].statistics.hiddenSubscriberCount') AS hiddenSubscriberCount,
    JSON_EXTRACT_SCALAR(response, '$.items[0].statistics.videoCount') AS video_count,

    JSON_EXTRACT_SCALAR(response, '$.items[0].status.privacyStatus') AS privacyStatus,
    JSON_EXTRACT_SCALAR(response, '$.items[0].status.isLinked') AS isLinked,
    JSON_EXTRACT_SCALAR(response, '$.items[0].kind') AS youtube_channel_type,
    JSON_EXTRACT_SCALAR(response, '$.items[0].customUrl') AS channel_custom_url
FROM
    raw_data
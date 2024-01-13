-- models/combined_data.sql

WITH
--  source_data AS (
----    SELECT * FROM {{ ref('source_data_quality') }}
--    SELECT * FROM {{ ref('source_data_quality') }}
----    SELECT * FROM FilmData.dbo.actor
--  ),
  destination_data AS (
    SELECT * FROM {{ source('target_db','raw_actor') }}
--    SELECT * FROM raw_film_data.raw_actor
  )
SELECT
--  source_data.*,
  destination_data.*
FROM
  source_data
JOIN
  destination_data
--ON
--  source_data.actor_id = destination_data.actor_id;

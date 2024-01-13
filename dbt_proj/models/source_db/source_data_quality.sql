-- models/combined_data.sql

WITH
  source_data AS (
--    SELECT * FROM {{ ref('actor', database='source_db') }} -- source.FilmData.dbo.actor
    SELECT * FROM {{ source('source_db','actor') }} -- source.FilmData.dbo.actor
--    SELECT * FROM FilmData.dbo.actor
  )
SELECT
  source_data.*
FROM
  source_data
 models/combined_data.sql

 models/combined_data.sql

--WITH
--  source_data AS (
--    SELECT * FROM {{ source('source_db', 'actor') }}
--  )
--SELECT
--  source_data.*
--FROM
--  source_data;
--SELECT * FROM {{ source('target_db','raw_actor') }}
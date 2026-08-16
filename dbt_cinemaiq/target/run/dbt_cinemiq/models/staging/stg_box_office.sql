
  create or replace   view CINEMAIQ.DEV_staging.stg_box_office
  
   as (
    -- stg_box_office.sql
-- Matches columns produced by ingestion/mojo.py scrape_year():
-- rank, title, year, worldwide_gross, domestic_gross, opening_weekend,
-- release_date, detail_url, scraped_at


WITH source AS (
    SELECT * FROM CINEMAIQ.RAW.raw_box_office
),

cleaned AS (
    SELECT
        TRIM(title) AS title,
        LOWER(TRIM(title)) AS title_normalized,
        TRY_CAST(rank AS INTEGER) AS rank,
        TRY_CAST(year AS INTEGER) AS release_year,
        NULLIF(TRY_CAST(worldwide_gross AS BIGINT), 0) AS worldwide_gross_usd,
        NULLIF(TRY_CAST(domestic_gross AS BIGINT), 0) AS domestic_gross_usd,
        NULLIF(TRY_CAST(opening_weekend AS BIGINT), 0) AS opening_weekend_usd,
        CASE
            WHEN TRY_CAST(worldwide_gross AS BIGINT) >= 500000000 THEN 'blockbuster'
            WHEN TRY_CAST(worldwide_gross AS BIGINT) >= 100000000 THEN 'wide_release'
            WHEN TRY_CAST(worldwide_gross AS BIGINT) >= 10000000 THEN 'limited_release'
            WHEN TRY_CAST(worldwide_gross AS BIGINT) > 0 THEN 'low_performer'
            ELSE 'unknown'
        END AS revenue_tier,
        TRIM(release_date) AS release_date_raw,
        TRIM(detail_url) AS source_url,
        TRY_CAST(scraped_at AS TIMESTAMP_NTZ) AS scraped_at
    FROM source
),

filtered AS (
    SELECT *
    FROM cleaned
    WHERE title IS NOT NULL
      AND release_year IS NOT NULL
      AND (
          worldwide_gross_usd IS NOT NULL
          OR domestic_gross_usd IS NOT NULL
      )
)

SELECT *
FROM filtered
  );


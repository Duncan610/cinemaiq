-- stg_news_articles.sql
-- Matches columns produced by ingestion/news_api.py flatten_article():
-- movie_query, article_id, source_name, author, headline, description, url,
-- published_at, positive_hits, negative_hits, sentiment_score, ingested_at


WITH source AS (
    SELECT * FROM CINEMAIQ.RAW.raw_news_articles
),

cleaned AS (
    SELECT
        TRIM(movie_query) AS movie_query,
        TRY_CAST(article_id AS VARCHAR) AS article_id,
        TRIM(source_name) AS source_name,
        TRIM(author) AS author,
        TRIM(headline) AS headline,
        TRIM(description) AS description,
        TRIM(url) AS url,
        TRY_CAST(published_at AS DATE) AS published_date,
        TRY_CAST(positive_hits AS INTEGER) AS positive_hits,
        TRY_CAST(negative_hits AS INTEGER) AS negative_hits,
        TRY_CAST(sentiment_score AS INTEGER) AS sentiment_score,
        CASE
            WHEN TRY_CAST(sentiment_score AS INTEGER) > 1 THEN 'positive'
            WHEN TRY_CAST(sentiment_score AS INTEGER) < -1 THEN 'negative'
            ELSE 'neutral'
        END AS sentiment_label,
        TRY_CAST(ingested_at AS TIMESTAMP_NTZ) AS ingested_at
    FROM source
),

filtered AS (
    SELECT *
    FROM cleaned
    WHERE movie_query IS NOT NULL
      AND headline IS NOT NULL
      AND headline != ''
)

SELECT *
FROM filtered
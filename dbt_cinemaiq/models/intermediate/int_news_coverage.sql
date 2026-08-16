/*
int_news_coverage.sql

WHAT THIS MODEL DOES:
stg_news_articles is one row per ARTICLE. This model aggregates that
up to one row per MOVIE total article count, unique outlets, average
sentiment. 

WHY THIS MATTERS FOR THE GOLD LAYER:
mart_prerelease_signals needs "how much media buzz did this movie get
before release" as a single number per movie, not a list of articles.
This model is that single number.

MATCHING TO int_movies_unified:
NewsAPI has no ID, only movie_query (the search term we used, which is
the TMDB title we searched with). We match on lowercased, trimmed title
against int_movies_unified's title. This is a LEFT JOIN FROM news
articles, not from movies so a movie with zero news coverage simply
never appears here, rather than appearing with null buzz metrics.

GRAIN: one row per movie_query (which maps ~1:1 to a movie title).
*/

WITH articles AS (
    SELECT *
    FROM {{ ref('stg_news_articles') }}
),

aggregated AS (
    SELECT
        movie_query,
        LOWER(TRIM(movie_query)) AS title_normalized,
        COUNT(DISTINCT article_id) AS total_articles,
        COUNT(DISTINCT source_name) AS unique_sources,
        AVG(sentiment_score) AS avg_sentiment_score,
        SUM(positive_hits) AS total_positive_hits,
        SUM(negative_hits) AS total_negative_hits,
        MIN(published_date) AS first_coverage_date,
        MAX(published_date) AS last_coverage_date,
        DATEDIFF(
            'day',
            MIN(published_date),
            MAX(published_date)
        ) AS coverage_span_days
    FROM articles
    GROUP BY movie_query
),

classified AS (
    SELECT
        *,
        ROUND(avg_sentiment_score, 2) AS avg_sentiment_rounded,
        CASE
            WHEN avg_sentiment_score > 0.5 THEN 'positive_buzz'
            WHEN avg_sentiment_score < -0.5 THEN 'negative_buzz'
            ELSE 'neutral_buzz'
        END AS buzz_sentiment_category,
        CASE
            WHEN total_articles >= 15 THEN 'high_coverage'
            WHEN total_articles >= 5 THEN 'medium_coverage'
            ELSE 'low_coverage'
        END AS coverage_volume_category
    FROM aggregated
)

SELECT *
FROM classified;
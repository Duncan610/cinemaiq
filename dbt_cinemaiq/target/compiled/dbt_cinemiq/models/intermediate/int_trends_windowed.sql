/*
int_trends_windowed.sql

WHAT THIS MODEL DOES:
stg_google_trends is one row per (movie, date) a daily interest score
time series. This model collapses that time series into one row per
movie, capturing the shape of interest rather than every daily point:
peak score, average score, and which window (3-month vs 12-month) it
came from.

WHY BOTH WINDOWS MATTER:
The 3-month window has finer resolution near a movie's release date.
The 12-month window shows the longer buildup (or lack of one) for
big franchise films announced far in advance. We keep them separate
rather than blending a mart can choose which one it needs.

GRAIN: one row per (movie_title_normalized, window_type).
So a movie can appear twice: once for its 3-month trend summary,
once for its 12-month trend summary.
*/

WITH trends AS (
    SELECT *
    FROM CINEMAIQ.DEV_staging.stg_google_trends
),

aggregated AS (
    SELECT
        movie_title_normalized,
        window_type,
        MAX(interest_score) AS peak_interest_score,
        AVG(interest_score) AS avg_interest_score,
        MIN(trend_date) AS window_start_date,
        MAX(trend_date) AS window_end_date,
        COUNT(*) AS data_points
    FROM trends
    GROUP BY movie_title_normalized, window_type
),

classified AS (
    SELECT
        *,
        ROUND(avg_interest_score, 1) AS avg_interest_rounded,
        CASE
            WHEN peak_interest_score >= 75 THEN 'viral'
            WHEN peak_interest_score >= 40 THEN 'strong'
            WHEN peak_interest_score >= 10 THEN 'moderate'
            ELSE 'minimal'
        END AS peak_interest_category
    FROM aggregated
)

SELECT *
FROM classified
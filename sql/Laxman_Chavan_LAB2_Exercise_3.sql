#standred sql
SELECT
  date,
  ARRAY_AGG(h.page.pagePath) AS pagePath,
  ARRAY_LENGTH(ARRAY_AGG(h.page.pagePath)) AS counter
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,UNNEST(hits) AS h
GROUP BY date
ORDER BY date
		
#standred sql
SELECT
  PARSE_DATE("%Y%m%d", date) AS date,
  h.page.pagePath AS pagePath,
  count(h.page.pagePath) AS counter
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,UNNEST(hits) AS h
GROUP BY date,pagePath
ORDER BY date,counter DESC
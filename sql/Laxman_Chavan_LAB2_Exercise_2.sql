#standred sql
SELECT DISTINCT
  visitId,
  visitStartTime,
  h.page.pageTitle,
  h.page.pagePath,
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`,
UNNEST(hits) AS h
WHERE date = "20170801"
LIMIT 10

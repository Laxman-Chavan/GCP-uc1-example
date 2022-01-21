#standred sql
SELECT
  L.name AS language_name,
  COUNT(L.name) AS count
  FROM `bigquery-public-data.github_repos.languages`,UNNEST(language) AS L
  GROUP BY language_name
  ORDER BY count DESC
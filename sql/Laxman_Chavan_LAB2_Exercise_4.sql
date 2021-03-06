#standred sql
with row_count AS (
SELECT
device.browser AS browser,
device.operatingSystem AS operating_System,
geoNetwork.country as country,
ROW_NUMBER() OVER (PARTITION BY  geoNetwork.country 
ORDER BY (count(device.browser)+count(device.operatingSystem)) ) AS rank
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`
WHERE device.isMobile=TRUE 
AND NOT geoNetwork.country='(not set)' 
AND NOT device.operatingSystem='(not set)' 
AND NOT device.browser='(not set)'
GROUP BY country,operating_System,browser
)
SELECT country,
ARRAY_AGG(STRUCT (operating_System,browser,rank)) AS country_rank 
FROM row_count WHERE rank<=3 
GROUP BY country
ORDER BY country






  


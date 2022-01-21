#standred sql
SELECT 
commits.committer.name AS name,
COUNT (L.name) AS count
FROM `bigquery-public-data.github_repos.languages`AS languages , UNNEST(language) AS L
INNER JOIN 
`bigquery-public-data.github_repos.sample_commits` AS commits 
ON languages.repo_name=commits.repo_name
WHERE L.name="Java" AND EXTRACT(YEAR FROM commits.committer.date)= 2016
GROUP BY name 
ORDER BY count DESC 

#standred sql
select 
repo_name,
d.old_path AS file,
committer.date AS date,
#LAG function looks backwards from current row to fetch value
LAG(commit)OVER(ORDER BY committer.date) AS previous_commit,
commit ,
#LEAD function looks forward from the current row to fetch value
LEAD(commit)OVER(ORDER BY committer.date)AS next_commit
FROM `bigquery-public-data.github_repos.sample_commits` , UNNEST(difference) AS d 
WHERE d.old_path="kernel/acct.c"
ORDER BY date

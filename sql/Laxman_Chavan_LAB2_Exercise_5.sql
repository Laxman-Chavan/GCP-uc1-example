#standred sql
SELECT 
users.id AS id_user, 
COUNT (postsAns.owner_user_id) AS count
FROM `bigquery-public-data.stackoverflow.users`AS users 
INNER JOIN 
`bigquery-public-data.stackoverflow.posts_answers` AS postsAns 
ON users.id=postsAns.owner_user_id
WHERE EXTRACT(YEAR FROM postsAns.creation_date)= 2010
GROUP BY id_user 
ORDER BY count DESC 
LIMIT 10
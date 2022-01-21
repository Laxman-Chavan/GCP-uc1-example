#standred sql
SELECT
users.id AS id_user,
COUNT(postAnst.owner_user_id) AS count
FROM
`bigquery-public-data.stackoverflow.users` users
INNER JOIN
`bigquery-public-data.stackoverflow.posts_answers` postAnst
ON users.id=postAnst.owner_user_id
INNER JOIN
`bigquery-public-data.stackoverflow.stackoverflow_posts` stackt
ON
stackt.accepted_answer_id=postAnst.id
WHERE stackt.accepted_answer_id IS NOT NULL AND EXTRACT(YEAR FROM stackt.creation_date)=2010
GROUP BY id_user
ORDER BY count DESC



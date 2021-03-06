#standred sql
with row_count as(
select year,Movie_Title,Production_Budget,RANK() OVER(PARTITION BY year order by Production_Budget DESC) AS rank
from
(SELECT
EXTRACT(YEAR FROM Release_Date) AS year,Movie_Title,Production_Budget
FROM `nttdata-c4e-bde.uc1_2.movie`
where Release_Date BETWEEN '2016-01-01' and '2020-12-31'
)
)
SELECT
year,Movie_Title,Production_Budget,rank
FROM row_count
WHERE rank < 11
ORDER BY year DESC 
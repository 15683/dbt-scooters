SELECT
	count(*) AS trips,
	count(DISTINCT t.user_id) AS users,
	avg(EXTRACT(epoch FROM (finished_at - started_at)))/ 60 AS avg_duration_m,
	SUM(t.price)/ 100 AS revenue_rub,
	count(price = 0 OR NULL)/ CAST(count(*) AS REAL)* 100 AS free_trips_pct
FROM
	scooters_raw.trips t

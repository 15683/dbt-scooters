SELECT
	date(t.started_at) AS date,
	count(*) AS trips,
	MAX(t.price)/ 100 AS max_price_rub,
	AVG(t.distance)/ 1000 AS avg_distance_km
FROM
	scooters_raw.trips t
GROUP BY
	date(t.started_at)
ORDER BY
	date(t.started_at)

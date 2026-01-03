/*SELECT
	count(*) AS trips,
	count(DISTINCT t.user_id) AS users,
	avg(EXTRACT(epoch FROM (finished_at - started_at)))/ 60 AS avg_duration_m,
	SUM(t.price)/ 100 AS revenue_rub,
	count(price = 0 OR NULL)/ CAST(count(*) AS REAL)* 100 AS free_trips_pct
FROM
	scooters_raw.trips t*/

	{{ config(
    materialized='table',
    tags=['metrics', 'trips']
) }}

{#
    Общая статистика по поездкам:
    - количество поездок и уникальных пользователей
    - средняя продолжительность
    - выручка
    - процент бесплатных поездок
#}

with trip_stats as (
    select
        count(*) as trips,
        count(distinct user_id) as users,
        avg(extract(epoch from (finished_at - started_at))) / 60 as avg_duration_m,
        sum(price) / 100.0 as revenue_rub,
        count(case when price = 0 then 1 end) as free_trips,
        count(*) as total_trips
    from {{ source('scooters_raw', 'trips') }}
)

select
    trips,
    users,
    avg_duration_m,
    revenue_rub,
    round((free_trips::numeric / nullif(total_trips, 0)) * 100, 2) as free_trips_pct
from trip_stats

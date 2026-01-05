/*SELECT
	date(t.started_at) AS date,
	count(*) AS trips,
	MAX(t.price)/ 100 AS max_price_rub,
	AVG(t.distance)/ 1000 AS avg_distance_km
FROM
	scooters_raw.trips t
GROUP BY
	date(t.started_at)
ORDER BY
	date(t.started_at)*/

	{{ config(
    materialized='incremental',
    unique_key='date',
    on_schema_change='append_new_columns',
    tags=['daily', 'metrics']
) }}

{#
    Ежедневная статистика поездок:
    - количество поездок
    - максимальная цена
    - средняя дистанция
#}

select
    date(started_at) as date,
    count(*) as trips,
    max(price) / 100 as max_price_rub,
    avg(distance) / 1000 as avg_distance_km
from {{ source('scooters_raw', 'trips') }}

{%- if is_incremental() %}
    {# Инкрементальная загрузка только новых данных #}
    where date(started_at) > (select max(date) from {{ this }})
{%- endif %}

group by date(started_at)
order by date

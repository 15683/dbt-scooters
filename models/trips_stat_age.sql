/*with
    date_age_cte as (
        select
            date(t.started_at) as date,
            extract(year from t.started_at) - extract(year from u.birth_date) as age
        from
            scooters_raw.trips as t
            inner join scooters_raw.users as u on t.user_id = u.id
    ),
    count_cte as (
        select
            date,
            age,
            count(*) as trips
        from
            date_age_cte
        group by
            1,
            2
        )
select
    age,
    avg(trips) as avg_trips
from
    count_cte
group by
    1
order by
    1*/

    {{ config(
        materialized='table',
        tags=['analysis', 'users']
    ) }}

    {#
        Анализ среднего количества поездок по возрастным группам
    #}

    with date_age_data as (
        select
            date(t.started_at) as trip_date,
            extract(year from age(t.started_at, u.birth_date)) as user_age
        from {{ source('scooters_raw', 'trips') }} as t
        inner join {{ source('scooters_raw', 'users') }} as u
            on t.user_id = u.id
        where u.birth_date is not null
    ),

    trips_by_date_age as (
        select
            trip_date,
            user_age,
            count(*) as daily_trips
        from date_age_data
        group by trip_date, user_age
    )

    select
        user_age as age,
        round(avg(daily_trips), 2) as avg_trips
    from trips_by_date_age
    group by user_age
    order by user_age

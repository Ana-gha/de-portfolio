-- models/marts/mart_daily_revenue.sql
-- Gold-layer equivalent: daily business metrics for BI dashboards
-- Informatica equivalent: Aggregator → Target warehouse table

{{
    config(
        materialized = 'table',
        description  = 'Daily revenue and trip metrics — BI-ready Gold table'
    )
}}

with trips as (

    select * from {{ ref('stg_trips') }}

),

daily_metrics as (

    select
        cast(pickup_datetime as date)               as trip_date,
        extract(year  from pickup_datetime)         as trip_year,
        extract(month from pickup_datetime)         as trip_month,
        extract(dow   from pickup_datetime)         as day_of_week,   -- 0=Sun, 6=Sat
        case
            when extract(dow from pickup_datetime) in (0, 6) then true
            else false
        end                                         as is_weekend,

        -- Volume
        count(*)                                    as trip_count,
        sum(passenger_count)                        as total_passengers,

        -- Revenue
        round(sum(fare_usd),  2)                    as total_fare_usd,
        round(sum(tip_usd),   2)                    as total_tips_usd,
        round(sum(total_usd), 2)                    as total_revenue_usd,

        -- Averages
        round(avg(fare_usd),       2)               as avg_fare_usd,
        round(avg(distance_miles), 2)               as avg_distance_miles,
        round(avg(
            extract(epoch from (dropoff_datetime - pickup_datetime)) / 60
        ), 1)                                       as avg_duration_mins,

        -- Quality metrics
        round(
            sum(case when tip_usd > 0 then 1 else 0 end)::numeric
            / count(*) * 100, 1
        )                                           as tip_rate_pct,

        round(avg(fare_usd / nullif(distance_miles, 0)), 2)
                                                    as avg_fare_per_mile

    from trips
    group by 1, 2, 3, 4, 5

),

with_rolling as (

    select
        *,

        -- 7-day rolling average revenue (window function)
        round(avg(total_revenue_usd) over (
            order by trip_date
            rows between 6 preceding and current row
        ), 2)                                       as revenue_7d_rolling_avg,

        -- Day-over-day change
        round(
            total_revenue_usd -
            lag(total_revenue_usd) over (order by trip_date),
        2)                                          as revenue_dod_change,

        -- Trip count rank (highest trip day = rank 1)
        rank() over (order by trip_count desc)      as trip_count_rank

    from daily_metrics

)

select * from with_rolling
order by trip_date

-- models/staging/stg_trips.sql
-- Staging model: rename, type cast, filter, deduplicate raw Bronze data
-- Informatica equivalent: Source Qualifier + Filter + Expression Transformation

{{
    config(
        materialized = 'view',
        description  = 'Cleaned and renamed NYC Taxi trips from Bronze layer'
    )
}}

with source as (

    select * from {{ source('raw', 'bronze_trips') }}

),

renamed as (

    select
        -- Surrogate key (combine vendor + pickup time + location for uniqueness)
        {{ dbt_utils.generate_surrogate_key([
            'VendorID', 'tpep_pickup_datetime', 'PULocationID'
        ]) }}                                           as trip_id,

        -- Dimensions
        VendorID                                        as vendor_id,
        tpep_pickup_datetime                            as pickup_datetime,
        tpep_dropoff_datetime                           as dropoff_datetime,
        PULocationID                                    as pickup_location_id,
        DOLocationID                                    as dropoff_location_id,
        passenger_count                                 as passenger_count,
        trip_distance                                   as distance_miles,
        payment_type                                    as payment_type_code,
        RatecodeID                                      as rate_code_id,

        -- Financials — rename and clean
        fare_amount                                     as fare_usd,
        tip_amount                                      as tip_usd,
        tolls_amount                                    as tolls_usd,
        improvement_surcharge                           as surcharge_usd,
        total_amount                                    as total_usd,
        coalesce(congestion_surcharge, 0)               as congestion_surcharge_usd,

        -- Metadata
        _ingestion_ts,
        _batch_id,
        _source_file

    from source

),

filtered as (

    select *
    from renamed
    where
        -- Valid dates
        pickup_datetime  is not null
        and dropoff_datetime is not null
        and dropoff_datetime > pickup_datetime

        -- Valid financials
        and fare_usd > {{ var('min_fare_usd') }}
        and fare_usd < {{ var('max_fare_usd') }}

        -- Valid trip
        and distance_miles > 0
        and passenger_count > 0
        and passenger_count <= {{ var('max_passengers') }}

        -- Date range filter (uses project variable)
        and cast(pickup_datetime as date) >= '{{ var("start_date") }}'

),

deduplicated as (

    -- Remove duplicates — keep latest ingested record
    -- Informatica equivalent: Aggregator with MAX(_ingestion_ts) per key
    select *
    from (
        select
            *,
            row_number() over (
                partition by trip_id
                order by _ingestion_ts desc
            ) as rn
        from filtered
    )
    where rn = 1

)

select * from deduplicated

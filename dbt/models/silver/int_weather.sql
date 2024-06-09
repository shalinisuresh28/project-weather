WITH raw_weather AS (

    SELECT *
    FROM {{ source('project_weather', 'raw_weather') }}

),

transformed AS (

    SELECT
        data.value:"parameter"::STRING AS parameter,
        coords.value:"lat"::FLOAT AS latitude,
        coords.value:"lon"::FLOAT AS longitude,
        DATE(dates.value:"date")::STRING AS date,
        dates.value:"value"::STRING AS value
    FROM
        raw_weather,
        LATERAL FLATTEN(input => COLUMN_1:"data") data,
        LATERAL FLATTEN(input => data.value:"coordinates") coords,
        LATERAL FLATTEN(input => coords.value:"dates") dates

)

SELECT * FROM transformed


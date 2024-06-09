WITH raw_weather AS (

    SELECT *
    FROM {{ source('project_weather', 'raw_weather') }}

),

transformed AS (

    SELECT
        data.value:"parameter"::STRING AS Parameter,
        coords.value:"lat"::FLOAT AS Latitude,
        coords.value:"lon"::FLOAT AS Longitude,
        DATE(dates.value:"date")::STRING AS Date,
        dates.value:"value"::STRING AS Value
    FROM
        raw_weather,
        LATERAL FLATTEN(input => COLUMN_1:"data") data,
        LATERAL FLATTEN(input => data.value:"coordinates") coords,
        LATERAL FLATTEN(input => coords.value:"dates") dates

)

SELECT * FROM transformed


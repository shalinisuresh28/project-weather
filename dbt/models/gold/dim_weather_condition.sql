WITH symbols AS (

    SELECT *
    FROM {{ref('seed_weather_symbols')}}

)

SELECT * FROM symbols
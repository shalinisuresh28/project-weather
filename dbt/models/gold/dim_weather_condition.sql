WITH symbols AS (

    SELECT
        Weather_Symbol_Id,
        Symbol_Description
    FROM {{ref('seed_weather_symbols')}}

)

SELECT * FROM symbols
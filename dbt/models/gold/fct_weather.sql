WITH weather AS (

    SELECT *
    FROM {{ref('int_weather')}}

),

transformed AS (

    SELECT 
        DATE,
        LATITUDE,
        LONGITUDE,
        MAX(CASE WHEN PARAMETER = 'weather_symbol_24h:idx' THEN VALUE END) AS Weather_Symbol_Id,
        MAX(CASE WHEN PARAMETER = 't_min_2m_24h:C' THEN VALUE END) AS Min_Temperature,
        MAX(CASE WHEN PARAMETER = 't_max_2m_24h:C' THEN VALUE END) AS Max_Temperature,
        MAX(CASE WHEN PARAMETER = 'precip_24h:mm' THEN VALUE END) AS Precipitation,
        MAX(CASE WHEN PARAMETER = 'wind_speed_10m:ms' THEN VALUE END) AS Wind_Speed,
        MAX(CASE WHEN PARAMETER = 'wind_dir_10m:d' THEN VALUE END) AS Wind_Direction,
        MAX(CASE WHEN PARAMETER = 'sunrise:sql' THEN VALUE END) AS Sunrise,
        MAX(CASE WHEN PARAMETER = 'sunset:sql' THEN VALUE END) AS Sunset,
        MAX(CASE WHEN PARAMETER = 'uv:idx' THEN VALUE END) AS UV_Index,
    FROM weather
    GROUP BY DATE, LATITUDE, LONGITUDE
    ORDER BY DATE, LATITUDE, LONGITUDE

),

final AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['Date', 'Latitude', 'Longitude']) }} AS Weather_Id,
        {{ dbt_utils.generate_surrogate_key(['Latitude', 'Longitude']) }} AS Location_Id,
        Date,
        Weather_Symbol_Id,
        Min_Temperature,
        Max_Temperature,
        Precipitation,
        Wind_Speed,
        Wind_Direction,
        Sunrise,
        Sunset,
        UV_Index
    FROM transformed

)

SELECT * FROM final
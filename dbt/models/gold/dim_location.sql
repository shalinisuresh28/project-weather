WITH location AS (

    SELECT *
    FROM {{ref('seed_location')}}

),

final AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['Latitude', 'Longitude']) }} AS Location_Id,
        City,
        Country,
        Latitude,
        Longitude
    FROM location

)

SELECT * FROM final
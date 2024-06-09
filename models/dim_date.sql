WITH date_spine AS (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="current_date()",
        end_date="dateadd(day, 10, current_date())"
    ) }}

),

final AS (
    
    SELECT
    date_day,
    EXTRACT(year FROM date_day) AS year,
    EXTRACT(month FROM date_day) AS month,
    EXTRACT(day FROM date_day) AS day,
    dayname(date_day) AS day_name,
    EXTRACT(dow FROM date_day) AS day_of_week,
    EXTRACT(week FROM date_day) AS week_of_year,
    CASE WHEN EXTRACT(dow FROM date_day) IN (0, 6) THEN 'Weekend' ELSE 'Weekday' END AS is_weekend
    FROM date_spine

)

SELECT * FROM final

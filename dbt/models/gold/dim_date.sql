WITH date_spine AS (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="dateadd(day, -1, current_date())",
        end_date="dateadd(day, 9, current_date())"
    ) }}

),

final AS (
    
    SELECT
        date_day AS Date,
        EXTRACT(year FROM date_day) AS Year,
        EXTRACT(month FROM date_day) AS Month,
        EXTRACT(day FROM date_day) AS Day,
        dayname(date_day) AS Day_Name,
        EXTRACT(dow FROM date_day) AS Day_of_Week,
        EXTRACT(week FROM date_day) AS Week_of_Year,
        CASE 
            WHEN EXTRACT(dow FROM date_day) IN (0, 6) THEN 'Weekend' 
            ELSE 'Weekday' 
        END AS Is_Weekend
    FROM date_spine

)

SELECT * FROM final

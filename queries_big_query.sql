#Big Query:

-- VW_AVG_TIME_SATURDAY_SUNDAY

SELECT AVG(Trip_time) AVG_TRIP_SATURDAY_SUNDAY, 
       weekday_name_full
FROM (       
SELECT 
       TIMESTAMP_DIFF(cast(dropoff_datetime as timestamp),cast(pickup_datetime as timestamp), MINUTE) Trip_time, 
       FORMAT_DATE('%A', timestamp (pickup_datetime)) AS weekday_name_full
FROM `data-sprints-322619.data_sprints.trips_vendors` 
) TAB
WHERE UPPER(weekday_name_full) IN ('SATURDAY', 'SUNDAY')
GROUP BY weekday_name_full

-- VW_AVG_TRIP

SELECT  AVG(trip_distance) as AVG_Distance_Traveled
FROM `data-sprints-322619.data_sprints.trips_vendors` 
WHERE passenger_count <=2;

-- VW_BIGGEST_VENDORS

SELECT SUM(total_amount) As Total_Amount, name AS Vendor_Name
FROM `data-sprints-322619.data_sprints.trips_vendors` 
GROUP BY vendor_id, name
ORDER BY total_Amount DESC
LIMIT 3

-- VW_HISTOGRAM_CASH

SELECT Total, 
       Pickup_Year_Month
FROM 
(       
    SELECT count(*) Total, FORMAT_DATE("%Y-%m-%d",timestamp(pickup_datetime)) AS Pickup_Year_Month
    FROM `data-sprints-322619.data_sprints.trips_vendors` 
    WHERE trim(upper(payment_type))='CASH'
    GROUP BY FORMAT_DATE("%Y-%m-%d", timestamp(pickup_datetime))
) TAB 
ORDER BY Pickup_Year_Month


-- VW_SERIES
SELECT count(*) Total, FORMAT_DATE("%Y-%m-%d", TIMESTAMP(pickup_datetime)) AS Day
FROM `data-sprints-322619.data_sprints.trips_vendors` 
WHERE pickup_datetime_year=2012 
AND pickup_datetime_month in (10,9,8)
GROUP BY FORMAT_DATE("%Y-%m-%d",TIMESTAMP (pickup_datetime))


##############
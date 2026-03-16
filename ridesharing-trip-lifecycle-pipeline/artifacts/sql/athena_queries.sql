-- Example Athena queries for ETL-Project-3

-- 1) Total completed trips per hour
SELECT
  event_hour,
  SUM(trip_count) AS total_completed_trips
FROM etl_project_3_analytics.trip_metrics_hourly_zone_metrics
GROUP BY event_hour
ORDER BY event_hour DESC;

-- 2) Top pickup/dropoff pairs by fare
SELECT
  pickup_location_id,
  dropoff_location_id,
  SUM(fare_sum) AS total_fare,
  SUM(trip_count) AS total_trips
FROM etl_project_3_analytics.trip_metrics_hourly_zone_metrics
GROUP BY pickup_location_id, dropoff_location_id
ORDER BY total_fare DESC
LIMIT 20;

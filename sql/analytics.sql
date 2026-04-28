    DROP TABLE IF EXISTS transport_weather_analytics;

    CREATE TABLE transport_weather_analytics AS
    SELECT
        d.route_id,
        d.stop_id,
        d.delay_minutes,
        w.temperature,
        w.windspeed,
        w.rainfall,
        d.timestamp::timestamp AS timestamp
    FROM fact_transport_delays d
    JOIN fact_weather w 
    ON ABS(EXTRACT(EPOCH FROM (d.timestamp::timestamp - w.timestamp::timestamp))) < 86400; -- Join on records within 10 minutes of each other 
    -- This table will allow to analyze how weather conditions correlate with transport delays, enabling us to identify patterns and potential causes of delays.
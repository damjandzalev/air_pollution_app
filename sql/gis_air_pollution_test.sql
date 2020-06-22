
select count(*)
from air_pollution.prediction_pollutant;

CREATE OR REPLACE VIEW air_pollution.measurements20200620 AS(
    SELECT pollutant_id,
       SENSOR.ID AS sensor_id,
       CASE WHEN 0 = SUM((30-ABS(EXTRACT('MINUTE' FROM air_pollution.normalizemeasurementtime(stamp)-air_pollution.normalizemeasurementtimerounded(stamp)))))
           THEN AVG(AMOUNT)
           ELSE SUM((30-ABS(EXTRACT('MINUTE' FROM air_pollution.normalizemeasurementtime(stamp)-air_pollution.normalizemeasurementtimerounded(stamp))))*AMOUNT)
           /SUM((30-ABS(EXTRACT('MINUTE' FROM air_pollution.normalizemeasurementtime(stamp)-air_pollution.normalizemeasurementtimerounded(stamp)))))
           END AS amount,
       air_pollution.normalizemeasurementtimerounded(stamp) as time
    FROM (
        SELECT ROUND(LATITUDE::numeric,7) AS latitude,
               ROUND(LONGITUDE::numeric,7) AS longitude,
               stamp,
               pollutant.id AS pollutant_id,
               VALUE::DOUBLE PRECISION AS AMOUNT
        FROM
            imports.measurements20200620
            INNER JOIN
            air_pollution.pollutant
            ON lower(pollutant.name) = lower(type) OR
               lower(pollutant.pollutant) = lower(type) OR
               lower(pollutant.description) = lower(type)) AS T
            INNER JOIN
            air_pollution.sensor ON sensor.latitude = T.latitude AND
                                    sensor.longitude = T.longitude
    GROUP BY pollutant_id, sensor_id, TIME
    ORDER BY TIME
);
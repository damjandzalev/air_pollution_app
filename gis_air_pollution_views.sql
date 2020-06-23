CREATE OR REPLACE VIEW air_pollution.SIM_DATA20200520 AS(
    SELECT simulation_run.ID AS SIM_R_ID, simulation_point.ID AS SIM_P_ID, pollutant_id, amount
    FROM imports.simulation_data20200520 AS SIM
        INNER JOIN
        air_pollution.simulation_point
        ON SIM.latitude = simulation_point.latitude AND
           SIM.longitude = simulation_point.longitude
        INNER JOIN
        air_pollution.simulation_run
        ON simulation_run.TIME = SUBSTR(SIM.time,12,20)::TIMESTAMP AND
           simulation_run.run_time = substr(sim."time", 12, 20)::timestamp
);

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
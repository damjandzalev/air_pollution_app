create index idx_sim_p on air_pollution.prediction_pollutant(simulation_point_id);
CREATE OR REPLACE VIEW air_pollution.SENSOR_PREDICTION_INTERPOLATION AS
(
    SELECT T1.sensor_id, T1.HOUR, T1.pollutant_id,T1.amount as prediction,pollutant_measurement.amount AS measured, pollutant_measurement.time
FROM
    (
        SELECT *
            FROM
                 (
                    SELECT sensor_id, extract('hour' from time) as hour, 1 as pollutant_id, sum(p1*(5.0-distance))/sum(5.0-distance) as amount
                    FROM (air_pollution.sensor
                        INNER JOIN
                        air_pollution.sensor_distance on sensor.id = sensor_distance.sensor_id) as points
                        inner join
                        air_pollution.prediction_pollutant on points.simulation_point_id = prediction_pollutant.simulation_point_id
                        inner join
                        air_pollution.simulation_run on prediction_pollutant.simulation_run_id = simulation_run.id
                        GROUP BY sensor_id, hour
                ) as t1
                UNION ALL
                (
                    SELECT sensor_id, extract('hour' from time) as hour, 2 as pollutant_id, sum(p2*(5.0-distance))/sum(5.0-distance) as amount
                    FROM (air_pollution.sensor
                        INNER JOIN
                        air_pollution.sensor_distance on sensor.id = sensor_distance.sensor_id) as points
                        inner join
                        air_pollution.prediction_pollutant on points.simulation_point_id = prediction_pollutant.simulation_point_id
                        inner join
                        air_pollution.simulation_run on prediction_pollutant.simulation_run_id = simulation_run.id
                        GROUP BY sensor_id, hour
                )
                UNION ALL
                (
                    SELECT sensor_id, extract('hour' from time) as hour, 3 as pollutant_id, sum(p3*(5.0-distance))/sum(5.0-distance) as amount
                    FROM (air_pollution.sensor
                        INNER JOIN
                        air_pollution.sensor_distance on sensor.id = sensor_distance.sensor_id) as points
                        inner join
                        air_pollution.prediction_pollutant on points.simulation_point_id = prediction_pollutant.simulation_point_id
                        inner join
                        air_pollution.simulation_run on prediction_pollutant.simulation_run_id = simulation_run.id
                        GROUP BY sensor_id, hour
                )
                UNION ALL
                (
                    SELECT sensor_id, extract('hour' from time) as hour, 4 as pollutant_id, sum(p4*(5.0-distance))/sum(5.0-distance) as amount
                    FROM (air_pollution.sensor
                        INNER JOIN
                        air_pollution.sensor_distance on sensor.id = sensor_distance.sensor_id) as points
                        inner join
                        air_pollution.prediction_pollutant on points.simulation_point_id = prediction_pollutant.simulation_point_id
                        inner join
                        air_pollution.simulation_run on prediction_pollutant.simulation_run_id = simulation_run.id
                        GROUP BY sensor_id, hour
                )
                UNION ALL
                (
                    SELECT sensor_id, extract('hour' from time) as hour, 5 as pollutant_id, sum(p5*(5.0-distance))/sum(5.0-distance) as amount
                    FROM (air_pollution.sensor
                        INNER JOIN
                        air_pollution.sensor_distance on sensor.id = sensor_distance.sensor_id) as points
                        inner join
                        air_pollution.prediction_pollutant on points.simulation_point_id = prediction_pollutant.simulation_point_id
                        inner join
                        air_pollution.simulation_run on prediction_pollutant.simulation_run_id = simulation_run.id
                        GROUP BY sensor_id, hour
                )
                UNION ALL
                (
                    SELECT sensor_id, extract('hour' from time) as hour, 6 as pollutant_id, sum(p6*(5.0-distance))/sum(5.0-distance) as amount
                    FROM (air_pollution.sensor
                        INNER JOIN
                        air_pollution.sensor_distance on sensor.id = sensor_distance.sensor_id) as points
                        inner join
                        air_pollution.prediction_pollutant on points.simulation_point_id = prediction_pollutant.simulation_point_id
                        inner join
                        air_pollution.simulation_run on prediction_pollutant.simulation_run_id = simulation_run.id
                        GROUP BY sensor_id, hour
                )
                UNION ALL
                (
                    SELECT sensor_id, extract('hour' from time) as hour, 7 as pollutant_id, sum(p7*(5.0-distance))/sum(5.0-distance) as amount
                    FROM (air_pollution.sensor
                        INNER JOIN
                        air_pollution.sensor_distance on sensor.id = sensor_distance.sensor_id) as points
                        inner join
                        air_pollution.prediction_pollutant on points.simulation_point_id = prediction_pollutant.simulation_point_id
                        inner join
                        air_pollution.simulation_run on prediction_pollutant.simulation_run_id = simulation_run.id
                        GROUP BY sensor_id, hour
                )
                UNION ALL
                (
                    SELECT sensor_id, extract('hour' from time) as hour, 8 as pollutant_id, sum(p8*(5.0-distance))/sum(5.0-distance) as amount
                    FROM (air_pollution.sensor
                        INNER JOIN
                        air_pollution.sensor_distance on sensor.id = sensor_distance.sensor_id) as points
                        inner join
                        air_pollution.prediction_pollutant on points.simulation_point_id = prediction_pollutant.simulation_point_id
                        inner join
                        air_pollution.simulation_run on prediction_pollutant.simulation_run_id = simulation_run.id
                        GROUP BY sensor_id, hour
                )
                UNION ALL
                (
                    SELECT sensor_id, extract('hour' from time) as hour, 9 as pollutant_id, sum(p9*(5.0-distance))/sum(5.0-distance) as amount
                    FROM (air_pollution.sensor
                        INNER JOIN
                        air_pollution.sensor_distance on sensor.id = sensor_distance.sensor_id) as points
                        inner join
                        air_pollution.prediction_pollutant on points.simulation_point_id = prediction_pollutant.simulation_point_id
                        inner join
                        air_pollution.simulation_run on prediction_pollutant.simulation_run_id = simulation_run.id
                        GROUP BY sensor_id, hour
                )
                UNION ALL
                (
                    SELECT sensor_id, extract('hour' from time) as hour, 10 as pollutant_id, sum(p10*(5.0-distance))/sum(5.0-distance) as amount
                    FROM (air_pollution.sensor
                        INNER JOIN
                        air_pollution.sensor_distance on sensor.id = sensor_distance.sensor_id) as points
                        inner join
                        air_pollution.prediction_pollutant on points.simulation_point_id = prediction_pollutant.simulation_point_id
                        inner join
                        air_pollution.simulation_run on prediction_pollutant.simulation_run_id = simulation_run.id
                        GROUP BY sensor_id, hour
                )
                UNION ALL
                (
                    SELECT sensor_id, extract('hour' from time) as hour, 11 as pollutant_id, sum(p11*(5.0-distance))/sum(5.0-distance) as amount
                    FROM (air_pollution.sensor
                        INNER JOIN
                        air_pollution.sensor_distance on sensor.id = sensor_distance.sensor_id) as points
                        inner join
                        air_pollution.prediction_pollutant on points.simulation_point_id = prediction_pollutant.simulation_point_id
                        inner join
                        air_pollution.simulation_run on prediction_pollutant.simulation_run_id = simulation_run.id
                        GROUP BY sensor_id, hour
                )
    ) AS T1
    INNER JOIN
    air_pollution.POLLUTANT_MEASUREMENT ON T1.sensor_id = pollutant_measurement.sensor_id AND EXTRACT('HOUR' FROM TIME) = t1.hour AND T1.pollutant_id = pollutant_measurement.pollutant_id
);

--kaj koja merna stanica sekoj pollutant za sekoj cas ima najgolem - najmal prosek
CREATE OR REPLACE VIEW MIN_MAX_PER_HOUR_MEASUREMENTS AS(
    SELECT averages.p_name, averages.hour, averages.s_name as max_station, averages2.s_name as min_station
    FROM
        (SELECT averages.p_name, hour, max(avg) as max, min(avg) as min
        FROM
            (SELECT pollutant.name as p_name, EXTRACT(HOUR FROM time) as hour, avg(amount) AS avg
            FROM pollutant_measurement INNER JOIN
                 pollutant ON pollutant_id = pollutant.id INNER JOIN
                 sensor ON sensor.id = sensor_id
            GROUP BY sensor.name, pollutant.name, hour) AS averages
        GROUP BY p_name, hour) AS maximums
        INNER JOIN
        (SELECT sensor.name as s_name, pollutant.name as p_name, EXTRACT(HOUR FROM time) as hour, avg(amount) AS avg
            FROM pollutant_measurement INNER JOIN
                 pollutant ON pollutant_id = pollutant.id INNER JOIN
                 sensor ON sensor.id = sensor_id
            GROUP BY sensor.name, pollutant.name, hour) AS averages
        ON MAXIMUMS.p_name = averages.p_name AND MAXIMUMS.HOUR = averages.HOUR AND MAX = AVG
        INNER JOIN
        (SELECT sensor.name as s_name, pollutant.name as p_name, EXTRACT(HOUR FROM time) as hour, avg(amount) AS avg
            FROM pollutant_measurement INNER JOIN
                 pollutant ON pollutant_id = pollutant.id INNER JOIN
                 sensor ON sensor.id = sensor_id
            GROUP BY sensor.name, pollutant.name, hour) AS averages2
        ON MAXIMUMS.p_name = averages2.p_name AND MAXIMUMS.HOUR = averages2.HOUR AND MIN = AVERAGES2.AVG
    ORDER BY p_name, hour
);

alter table sensor add column geometry_column geometry;

update sensor set geometry_column=ST_Transform(ST_SetSRID(ST_MakePoint(longitude, latitude), 4326),26913);

create view point_prediction_interpolation as (
select *
from interpolation(23,40));


select *
from point_prediction_interpolation;
CREATE OR REPLACE FUNCTION air_pollution.sensor_distance_sim_point_insert()--on sim point insert, inserts the distances to the sensors in sensor_distance
  RETURNS trigger AS
$BODY$
BEGIN
	INSERT INTO air_pollution.sensor_distance(simulation_point_id,sensor_id, distance)
    SELECT new.id AS sim_p, sensor.id AS sensor, air_pollution.st_distance(air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(sensor.longitude::float, sensor.latitude::float), 4326),26913), air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(new.longitude::float, new.latitude::float), 4326),26913))/1000 AS distance
    FROM air_pollution.sensor
    WHERE air_pollution.st_distance(air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(sensor.longitude::float, sensor.latitude::float), 4326),26913), air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(new.longitude::float, new.latitude::float), 4326),26913)) <= 5000;
	RETURN new;
END;
$BODY$
LANGUAGE plpgsql;


CREATE TRIGGER sensor_distance_sim_point_insert--on sim point insert, inserts the distances to the sensors in sensor_distance
    AFTER INSERT
    ON air_pollution.simulation_point
    FOR EACH ROW
    EXECUTE PROCEDURE air_pollution.sensor_distance_sim_point_insert();


CREATE OR REPLACE FUNCTION air_pollution.sensor_distance_sensor_insert()--on sensor insert, inserts the distances to the sim points in sensor_distance
  RETURNS trigger AS
$BODY$
BEGIN
	INSERT INTO air_pollution.sensor_distance(simulation_point_id,sensor_id, distance)
    SELECT simulation_point.id AS sim_p, new.id AS sensor, air_pollution.st_distance(air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(simulation_point.longitude::float, simulation_point.latitude::float), 4326),26913), air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(new.longitude::float, new.latitude::float), 4326),26913))/1000 AS distance
    FROM air_pollution.simulation_point
    WHERE air_pollution.st_distance(air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(simulation_point.longitude::float, simulation_point.latitude::float), 4326),26913), air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(new.longitude::float, new.latitude::float), 4326),26913)) <= 5000;
	RETURN new;
END;
$BODY$
LANGUAGE plpgsql;


CREATE TRIGGER sensor_distance_sensor_insert--on sensor insert, inserts the distances to the sim points in sensor_distance
    AFTER INSERT
    ON air_pollution.sensor
    FOR EACH ROW
    EXECUTE PROCEDURE air_pollution.sensor_distance_sensor_insert();


CREATE OR REPLACE FUNCTION air_pollution.interpolation(lon DOUBLE PRECISION, lat DOUBLE PRECISION)--finds the interpolation on the point with coordinates (lon,lat) from the predictions
    RETURNS TABLE(
        HOUR DOUBLE PRECISION,
        POLLUTANT_ID INTEGER,
        AMOUNT DOUBLE PRECISION) AS
$BODY$
BEGIN
    RETURN QUERY
    select extract (hour from simulation_run.time) as h,
       pol_id,
       sum(am*(5.0-amounts.distance))/sum(5.0-amounts.distance) as am--(5.0-distance) -> more weight to the closer points
    from
        (
            (
                select distance, simulation_run_id, 1 AS POL_ID, p1 as am
                from (
                    SELECT sim_p.id, air_pollution.st_distance(air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(lon, lat), 4326),26913), air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(sim_p.longitude, sim_p.latitude), 4326),26913))/1000 AS distance
                    FROM air_pollution.simulation_point AS sim_p
                    WHERE air_pollution.st_distance(air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(lon, lat), 4326),26913), air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(sim_p.longitude, sim_p.latitude), 4326),26913)) <= 5000
                ) as points inner join
                air_pollution.prediction_pollutant as pp on pp.simulation_point_id = points.id
            )
            UNION ALL
            (
                select distance, simulation_run_id, 2 AS POL_ID, p2 as am
                from (
                    SELECT sim_p.id, air_pollution.st_distance(air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(lon, lat), 4326),26913), air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(sim_p.longitude, sim_p.latitude), 4326),26913))/1000 AS distance
                    FROM air_pollution.simulation_point AS sim_p
                    WHERE air_pollution.st_distance(air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(lon, lat), 4326),26913), air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(sim_p.longitude, sim_p.latitude), 4326),26913)) <= 5000
                ) as points inner join
                air_pollution.prediction_pollutant as pp on pp.simulation_point_id = points.id
            )
            UNION ALL
            (
                select distance, simulation_run_id, 3 AS POL_ID, p3 as am
                from (
                    SELECT sim_p.id, air_pollution.st_distance(air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(lon, lat), 4326),26913), air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(sim_p.longitude, sim_p.latitude), 4326),26913))/1000 AS distance
                    FROM air_pollution.simulation_point AS sim_p
                    WHERE air_pollution.st_distance(air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(lon, lat), 4326),26913), air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(sim_p.longitude, sim_p.latitude), 4326),26913)) <= 5000
                ) as points inner join
                air_pollution.prediction_pollutant as pp on pp.simulation_point_id = points.id
            )
            UNION ALL
            (
                select distance, simulation_run_id, 4 AS POL_ID, p4 as am
                from (
                    SELECT sim_p.id, air_pollution.st_distance(air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(lon, lat), 4326),26913), air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(sim_p.longitude, sim_p.latitude), 4326),26913))/1000 AS distance
                    FROM air_pollution.simulation_point AS sim_p
                    WHERE air_pollution.st_distance(air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(lon, lat), 4326),26913), air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(sim_p.longitude, sim_p.latitude), 4326),26913)) <= 5000
                ) as points inner join
                air_pollution.prediction_pollutant as pp on pp.simulation_point_id = points.id
            )
            UNION ALL
            (
                select distance, simulation_run_id, 5 AS POL_ID, p5 as am
                from (
                    SELECT sim_p.id, air_pollution.st_distance(air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(lon, lat), 4326),26913), air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(sim_p.longitude, sim_p.latitude), 4326),26913))/1000 AS distance
                    FROM air_pollution.simulation_point AS sim_p
                    WHERE air_pollution.st_distance(air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(lon, lat), 4326),26913), air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(sim_p.longitude, sim_p.latitude), 4326),26913)) <= 5000
                ) as points inner join
                air_pollution.prediction_pollutant as pp on pp.simulation_point_id = points.id
            )
            UNION ALL
            (
                select distance, simulation_run_id, 6 AS POL_ID, p6 as am
                from (
                    SELECT sim_p.id, air_pollution.st_distance(air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(lon, lat), 4326),26913), air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(sim_p.longitude, sim_p.latitude), 4326),26913))/1000 AS distance
                    FROM air_pollution.simulation_point AS sim_p
                    WHERE air_pollution.st_distance(air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(lon, lat), 4326),26913), air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(sim_p.longitude, sim_p.latitude), 4326),26913)) <= 5000
                ) as points inner join
                air_pollution.prediction_pollutant as pp on pp.simulation_point_id = points.id
            )
            UNION ALL
            (
                select distance, simulation_run_id, 7 AS POL_ID, p7 as am
                from (
                    SELECT sim_p.id, air_pollution.st_distance(air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(lon, lat), 4326),26913), air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(sim_p.longitude, sim_p.latitude), 4326),26913))/1000 AS distance
                    FROM air_pollution.simulation_point AS sim_p
                    WHERE air_pollution.st_distance(air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(lon, lat), 4326),26913), air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(sim_p.longitude, sim_p.latitude), 4326),26913)) <= 5000
                ) as points inner join
                air_pollution.prediction_pollutant as pp on pp.simulation_point_id = points.id
            )
            UNION ALL
            (
                select distance, simulation_run_id, 8 AS POL_ID, p8 as am
                from (
                    SELECT sim_p.id, air_pollution.st_distance(air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(lon, lat), 4326),26913), air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(sim_p.longitude, sim_p.latitude), 4326),26913))/1000 AS distance
                    FROM air_pollution.simulation_point AS sim_p
                    WHERE air_pollution.st_distance(air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(lon, lat), 4326),26913), air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(sim_p.longitude, sim_p.latitude), 4326),26913)) <= 5000
                ) as points inner join
                air_pollution.prediction_pollutant as pp on pp.simulation_point_id = points.id
            )
            UNION ALL
            (
                select distance, simulation_run_id, 9 AS POL_ID, p9 as am
                from (
                    SELECT sim_p.id, air_pollution.st_distance(air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(lon, lat), 4326),26913), air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(sim_p.longitude, sim_p.latitude), 4326),26913))/1000 AS distance
                    FROM air_pollution.simulation_point AS sim_p
                    WHERE air_pollution.st_distance(air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(lon, lat), 4326),26913), air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(sim_p.longitude, sim_p.latitude), 4326),26913)) <= 5000
                ) as points inner join
                air_pollution.prediction_pollutant as pp on pp.simulation_point_id = points.id
            )
            UNION ALL
            (
                select distance, simulation_run_id, 10 AS POL_ID, p10 as am
                from (
                    SELECT sim_p.id, air_pollution.st_distance(air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(lon, lat), 4326),26913), air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(sim_p.longitude, sim_p.latitude), 4326),26913))/1000 AS distance
                    FROM air_pollution.simulation_point AS sim_p
                    WHERE air_pollution.st_distance(air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(lon, lat), 4326),26913), air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(sim_p.longitude, sim_p.latitude), 4326),26913)) <= 5000
                ) as points inner join
                air_pollution.prediction_pollutant as pp on pp.simulation_point_id = points.id
            )
            UNION ALL
            (
                select distance, simulation_run_id, 11 AS POL_ID, p11 as am
                from (
                    SELECT sim_p.id, air_pollution.st_distance(air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(lon, lat), 4326),26913), air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(sim_p.longitude, sim_p.latitude), 4326),26913))/1000 AS distance
                    FROM air_pollution.simulation_point AS sim_p
                    WHERE air_pollution.st_distance(air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(lon, lat), 4326),26913), air_pollution.ST_Transform(air_pollution.ST_SetSRID(air_pollution.ST_MakePoint(sim_p.longitude, sim_p.latitude), 4326),26913)) <= 5000
                ) as points inner join
                air_pollution.prediction_pollutant as pp on pp.simulation_point_id = points.id
            )
        ) as amounts inner join air_pollution.simulation_run on simulation_run_id = simulation_run.id
    GROUP BY H, POL_ID;
END;
$BODY$
LANGUAGE plpgsql;

--inserts the simulation_points that are missing in the database
CREATE OR REPLACE FUNCTION air_pollution.normalizeSimPoints (tbl REGCLASS) RETURNS BOOLEAN AS
$BODY$
BEGIN
    EXECUTE '
            INSERT INTO air_pollution.simulation_point(longitude, latitude)
            SELECT t.longitude, t.latitude
            FROM
                 (
                    SELECT DISTINCT longitude, latitude
                    FROM ' || tbl ||'
                ) AS t
                LEFT OUTER JOIN
                air_pollution.simulation_point AS sim_p
                on t.longitude = sim_p.longitude AND
                   t.latitude = sim_p.latitude
            WHERE sim_p.latitude IS NULL AND sim_p.longitude IS NULL';
    RETURN FALSE;
END
$BODY$
LANGUAGE plpgsql;

--inserts the new simulation_runs
CREATE OR REPLACE FUNCTION air_pollution.normalizeSimRuns (tbl REGCLASS, run_time TEXT) RETURNS BOOLEAN AS
$BODY$
BEGIN
    EXECUTE '
            INSERT INTO air_pollution.simulation_run(time,run_time)
            SELECT SUBSTR(time, 12,21)::TIMESTAMP,' || CHR(39) || RUN_TIME || CHR(39) ||' ::TIMESTAMP AS run_time
            FROM
                 (
                    select distinct time
                    from ' || tbl || '
                 ) as t';
    RETURN FALSE;
END
$BODY$
LANGUAGE plpgsql;

--creates a view that gives the simulation point id and the simulation run id for each row of the sim data
CREATE OR REPLACE FUNCTION air_pollution.normalizeSimDataView(tbl REGCLASS, run_time TEXT) RETURNS BOOLEAN AS
$BODY$
BEGIN
    EXECUTE 'CREATE OR REPLACE VIEW air_pollution.SIM_DATA'|| concat(substr(run_time,0,5),substr(run_time,6,2),substr(run_time,9,2)) ||' AS(
                SELECT simulation_run.ID AS SIM_R_ID, simulation_point.ID AS SIM_P_ID, pollutant_id, amount
                FROM ' || TBL ||' AS SIM
                    INNER JOIN
                    air_pollution.simulation_point
                    ON SIM.latitude = simulation_point.latitude AND
                       SIM.longitude = simulation_point.longitude
                    INNER JOIN
                    air_pollution.simulation_run
                    ON simulation_run.TIME = SUBSTR(SIM.time,12,20)::TIMESTAMP AND' ||
            '           simulation_run.run_time = '|| CHR(39) || RUN_TIME || CHR(39) ||' ::TIMESTAMP
            )';
    RETURN FALSE;
END
$BODY$
LANGUAGE plpgsql;

--inserts the new prediction_pollutants
CREATE OR REPLACE FUNCTION air_pollution.normalizeSimDataInsert(run_time TEXT) RETURNS BOOLEAN AS
$BODY$
BEGIN
    EXECUTE '
            INSERT INTO air_pollution.prediction_pollutant(SIMULATION_RUN_ID, SIMULATION_POINT_ID)
            SELECT DISTINCT SIM_R_ID, SIM_P_ID
            FROM
                air_pollution.SIM_DATA' || concat(substr(run_time,0,5),substr(run_time,6,2),substr(run_time,9,2));
    RETURN FALSE;
END
$BODY$
LANGUAGE plpgsql;


--inserts the amount for the pollutant for the right prediction_pollutant
CREATE OR REPLACE FUNCTION air_pollution.normalizeSimDataUpdate(p INTEGER, run_time TEXT) RETURNS BOOLEAN AS
$BODY$
BEGIN
    EXECUTE '
            UPDATE air_pollution.prediction_pollutant
            SET P'||p||' = amount
            FROM air_pollution.SIM_DATA' || concat(substr(run_time,0,5),substr(run_time,6,2),substr(run_time,9,2)) ||' as SIM_DATA
            WHERE SIM_DATA.pollutant_id = '||p||' AND
                  prediction_pollutant.simulation_point_id = SIM_DATA.SIM_P_ID AND
                  prediction_pollutant.simulation_run_id = SIM_DATA.SIM_R_ID';
    RETURN FALSE;
END
$BODY$
LANGUAGE plpgsql;


--normalizes the data
CREATE OR REPLACE FUNCTION air_pollution.normalizeSimData (tbl REGCLASS, run_time TEXT) RETURNS BOOLEAN AS
$BODY$
BEGIN
    PERFORM air_pollution.normalizeSimPoints(tbl);
    PERFORM air_pollution.normalizeSimRuns(tbl, run_time);
    PERFORM air_pollution.normalizeSimDataView(tbl,run_time);
    EXECUTE '
            INSERT INTO air_pollution.prediction_pollutant(SIMULATION_RUN_ID, SIMULATION_POINT_ID)
            SELECT DISTINCT SIM_R_ID, SIM_P_ID
            FROM
                air_pollution.SIM_DATA' || concat(substr(run_time,0,5),substr(run_time,6,2),substr(run_time,9,2));
    EXECUTE '
            UPDATE air_pollution.prediction_pollutant
            SET P1 = amount
            FROM air_pollution.SIM_DATA' || concat(substr(run_time,0,5),substr(run_time,6,2),substr(run_time,9,2)) ||' as SIM_DATA
            WHERE SIM_DATA.pollutant_id = 1 AND
                  prediction_pollutant.simulation_point_id = SIM_DATA.SIM_P_ID AND
                  prediction_pollutant.simulation_run_id = SIM_DATA.SIM_R_ID
            UPDATE air_pollution.prediction_pollutant
            SET P2 = amount
            FROM air_pollution.SIM_DATA' || concat(substr(run_time,0,5),substr(run_time,6,2),substr(run_time,9,2)) ||' as SIM_DATA
            WHERE SIM_DATA.pollutant_id = 2 AND
                  prediction_pollutant.simulation_point_id = SIM_DATA.SIM_P_ID AND
                  prediction_pollutant.simulation_run_id = SIM_DATA.SIM_R_ID
            UPDATE air_pollution.prediction_pollutant
            SET P3 = amount
            FROM air_pollution.SIM_DATA' || concat(substr(run_time,0,5),substr(run_time,6,2),substr(run_time,9,2)) ||' as SIM_DATA
            WHERE SIM_DATA.pollutant_id = 3 AND
                  prediction_pollutant.simulation_point_id = SIM_DATA.SIM_P_ID AND
                  prediction_pollutant.simulation_run_id = SIM_DATA.SIM_R_ID
            UPDATE air_pollution.prediction_pollutant
            SET P4 = amount
            FROM air_pollution.SIM_DATA' || concat(substr(run_time,0,5),substr(run_time,6,2),substr(run_time,9,2)) ||' as SIM_DATA
            WHERE SIM_DATA.pollutant_id = 4 AND
                  prediction_pollutant.simulation_point_id = SIM_DATA.SIM_P_ID AND
                  prediction_pollutant.simulation_run_id = SIM_DATA.SIM_R_ID
            UPDATE air_pollution.prediction_pollutant
            SET P5 = amount
            FROM air_pollution.SIM_DATA' || concat(substr(run_time,0,5),substr(run_time,6,2),substr(run_time,9,2)) ||' as SIM_DATA
            WHERE SIM_DATA.pollutant_id = 5 AND
                  prediction_pollutant.simulation_point_id = SIM_DATA.SIM_P_ID AND
                  prediction_pollutant.simulation_run_id = SIM_DATA.SIM_R_ID
            UPDATE air_pollution.prediction_pollutant
            SET P6 = amount
            FROM air_pollution.SIM_DATA' || concat(substr(run_time,0,5),substr(run_time,6,2),substr(run_time,9,2)) ||' as SIM_DATA
            WHERE SIM_DATA.pollutant_id = 6 AND
                  prediction_pollutant.simulation_point_id = SIM_DATA.SIM_P_ID AND
                  prediction_pollutant.simulation_run_id = SIM_DATA.SIM_R_ID
            UPDATE air_pollution.prediction_pollutant
            SET P7 = amount
            FROM air_pollution.SIM_DATA' || concat(substr(run_time,0,5),substr(run_time,6,2),substr(run_time,9,2)) ||' as SIM_DATA
            WHERE SIM_DATA.pollutant_id = 7 AND
                  prediction_pollutant.simulation_point_id = SIM_DATA.SIM_P_ID AND
                  prediction_pollutant.simulation_run_id = SIM_DATA.SIM_R_ID
            UPDATE air_pollution.prediction_pollutant
            SET P8 = amount
            FROM air_pollution.SIM_DATA' || concat(substr(run_time,0,5),substr(run_time,6,2),substr(run_time,9,2)) ||' as SIM_DATA
            WHERE SIM_DATA.pollutant_id = 8 AND
                  prediction_pollutant.simulation_point_id = SIM_DATA.SIM_P_ID AND
                  prediction_pollutant.simulation_run_id = SIM_DATA.SIM_R_ID
            UPDATE air_pollution.prediction_pollutant
            SET P9 = amount
            FROM air_pollution.SIM_DATA' || concat(substr(run_time,0,5),substr(run_time,6,2),substr(run_time,9,2)) ||' as SIM_DATA
            WHERE SIM_DATA.pollutant_id = 9 AND
                  prediction_pollutant.simulation_point_id = SIM_DATA.SIM_P_ID AND
                  prediction_pollutant.simulation_run_id = SIM_DATA.SIM_R_ID
            UPDATE air_pollution.prediction_pollutant
            SET P10 = amount
            FROM air_pollution.SIM_DATA' || concat(substr(run_time,0,5),substr(run_time,6,2),substr(run_time,9,2)) ||' as SIM_DATA
            WHERE SIM_DATA.pollutant_id = 10 AND
                  prediction_pollutant.simulation_point_id = SIM_DATA.SIM_P_ID AND
                  prediction_pollutant.simulation_run_id = SIM_DATA.SIM_R_ID
            UPDATE air_pollution.prediction_pollutant
            SET P11 = amount
            FROM air_pollution.SIM_DATA' || concat(substr(run_time,0,5),substr(run_time,6,2),substr(run_time,9,2)) ||' as SIM_DATA
            WHERE SIM_DATA.pollutant_id = 11 AND
                  prediction_pollutant.simulation_point_id = SIM_DATA.SIM_P_ID AND
                  prediction_pollutant.simulation_run_id = SIM_DATA.SIM_R_ID';
    RETURN FALSE;
END
$BODY$
LANGUAGE plpgsql;

--transforms the stamp from pulse.eco to a postgres timestamp with no time zone
CREATE OR REPLACE FUNCTION air_pollution.normalizeMeasurementTime(stamp TEXT)
    RETURNS TIMESTAMP AS
$BODY$
BEGIN
    RETURN
    (CASE
        WHEN substr(STAMP, 20, 1) = '+'
            THEN (concat(substr(STAMP, 1, 10), ' ' ,substr(STAMP, 12, 8))::timestamp - substr(STAMP, 21, 5)::interval)::TIMESTAMP
            ELSE (concat(substr(STAMP, 1, 10), ' ' ,substr(STAMP, 12, 8))::timestamp + substr(STAMP, 21, 5)::interval)::TIMESTAMP
        END
    )::timestamp;
END
$BODY$
LANGUAGE plpgsql;


--rounds the transformed timestamp from normalizeMeasurementTime to the closest hour
CREATE OR REPLACE FUNCTION air_pollution.normalizeMeasurementTimeRounded(stamp TEXT)
    RETURNS TIMESTAMP AS
$BODY$
BEGIN
    RETURN date_trunc('hour',air_pollution.normalizeMeasurementTime(stamp) + interval '30 minutes');
END
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION air_pollution.normalizeMeasurementDataView(run_time TEXT) RETURNS BOOLEAN AS
$BODY$
BEGIN
    EXECUTE 'CREATE OR REPLACE VIEW air_pollution.measurements20200620 AS(
    SELECT pollutant_id,
       SENSOR.ID AS sensor_id,
       CASE WHEN 0 = SUM((30-ABS(EXTRACT(''MINUTE'' FROM air_pollution.normalizemeasurementtime(stamp)-air_pollution.normalizemeasurementtimerounded(stamp)))))
           THEN AVG(AMOUNT)
           ELSE SUM((30-ABS(EXTRACT(''MINUTE'' FROM air_pollution.normalizemeasurementtime(stamp)-air_pollution.normalizemeasurementtimerounded(stamp))))*AMOUNT)
           /SUM((30-ABS(EXTRACT(''MINUTE'' FROM air_pollution.normalizemeasurementtime(stamp)-air_pollution.normalizemeasurementtimerounded(stamp)))))
           END AS amount,
       air_pollution.normalizemeasurementtimerounded(stamp) as time
    FROM (
        SELECT ROUND(LATITUDE::numeric,7) AS latitude,
               ROUND(LONGITUDE::numeric,7) AS longitude,
               stamp,
               pollutant.id AS pollutant_id,
               VALUE::DOUBLE PRECISION AS AMOUNT
        FROM
            imports.measurements'|| run_time ||'
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
);';
    RETURN FALSE;
END
$BODY$
LANGUAGE plpgsql;

--inserts the new prediction_pollutants
CREATE OR REPLACE FUNCTION air_pollution.normalizeMeasurementDataInsert(run_time TEXT) RETURNS BOOLEAN AS
$BODY$
BEGIN
    EXECUTE '
            INSERT INTO air_pollution.pollutant_measurement(pollutant_id, sensor_id, amount, time)
            SELECT  pollutant_id,
                    sensor_id,
                    amount,
                    time
            FROM
                air_pollution.measurements' || run_time;
    RETURN FALSE;
END
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION air_pollution.normalizeMeasurementData (run_time TEXT) RETURNS BOOLEAN AS
$BODY$
BEGIN
    PERFORM air_pollution.normalizeMeasurementDataView(run_time);
    PERFORM air_pollution.normalizeMeasurementDataInsert(run_time);
    RETURN FALSE;
END
$BODY$
LANGUAGE plpgsql;


--inserts sensors that are missing
CREATE OR REPLACE FUNCTION air_pollution.insertSensors(run_time TEXT) RETURNS BOOLEAN AS
$BODY$
BEGIN
    EXECUTE 'INSERT INTO air_pollution.sensor(endpoint_name, name, longitude, latitude)
        SELECT endpoint, description, T2.longitude, T2.latitude
        FROM
            (SELECT DISTINCT ''Pulse.eco'' as endpoint, ROUND(LONGITUDE::NUMERIC, 7) as longitude, ROUND(LATITUDE::NUMERIC, 7) as latitude
            FROM imports.measurements'|| run_time ||'
            WHERE NOT ROUND(LONGITUDE::NUMERIC, 7) IN (SELECT DISTINCT LONGITUDE
            FROM air_pollution.sensor) AND NOT  ROUND(LATITUDE::NUMERIC, 7) IN (SELECT DISTINCT LATITUDE
            FROM air_pollution.sensor) )AS T1 JOIN
            (SELECT DESCRIPTION, ROUND(LONGITUDE::NUMERIC, 7) as longitude, ROUND(LATITUDE::NUMERIC, 7) as latitude
            FROM IMPORTS.sensors'|| run_time ||') AS T2 ON T1.latitude = T2.latitude AND
                                                   T1.longitude = T2.longitude';
    RETURN FALSE;
END
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION air_pollution.normalizeSensorData (run_time TEXT) RETURNS BOOLEAN AS
$BODY$
BEGIN
    PERFORM air_pollution.insertSensors(run_time);
    RETURN FALSE;
END
$BODY$
LANGUAGE plpgsql;
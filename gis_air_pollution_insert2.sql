INSERT INTO air_pollution.POLLUTANT(ID,POLLUTANT,DESCRIPTION,NAME)
SELECT id, pollutant, description, name_mv FROM public.postgres_public_pollutant;


INSERT INTO air_pollution.POLLUTANT
VALUES (12,'O3','O3','O3'),
       (13,'CO2','CO2','CO2'),
       (14,'HUMIDITY','HUMIDITY','HUMIDITY'),
       (15,'NOISE_DBA','NOISE_DBA','NOISE_DBA'),
       (16,'PRESSURE','PRESSURE','PRESSURE'),
       (17,'TEMPERATURE','TEMPERATURE','TEMPERATURE');

INSERT INTO air_pollution.sensor(ENDPOINT_NAME, name, longitude, latitude)
SELECT c1, station_name, ROUND(LONGITUDE::numeric,7), ROUND(LATITUDE::numeric,7)
FROM public.postgres_public_sensors_mv;


INSERT INTO air_pollution.POLLUTANT_MEASUREMENT(pollutant_id, sensor_id, amount, time)
SELECT pollutant.ID, sensor.id, a.data, a.datetime
FROM public.postgres_public_measurements_2020 as a,
     air_pollution.POLLUTANT as pollutant,
     air_pollution.sensor
WHERE (pollutant.DESCRIPTION = a.pollutant or pollutant.name = a.pollutant or pollutant.pollutant = a.pollutant) and a.station = sensor.name;


INSERT INTO air_pollution.pollution_source(longitude, latitude, gnfr_aggregated_sectors, bix_no2_kt, nmvoc_kt, sox_so2_kt, nh3_kt, pm25_kt, pm10_kt, bc_kt, co_kt, pb_kt, cd_kt, hg_kt, pcdd_pcdf_dioxins_furans_gi_teq, pahs_t, hcb_kg, pcbs_kg, square_area, point_geo, total_are_km2, urban_area_km2, point_geom)
SELECT *
FROM public.postgres_public_gridded_emissions_mk_2015;


--new simulation_points
INSERT INTO air_pollution.simulation_point(longitude, latitude)
SELECT t.longitude, t.latitude
FROM
     (
        SELECT DISTINCT longitude, latitude
        FROM imports.simulation_data20200520
    ) AS t
    LEFT OUTER JOIN
    air_pollution.simulation_point AS sim_p
    on t.longitude = sim_p.longitude AND
       t.latitude = sim_p.latitude
WHERE sim_p.latitude IS NULL AND sim_p.longitude IS NULL;


--new simulation_runs
INSERT INTO air_pollution.simulation_run(time,run_time)
SELECT substr(time, 12,21)::TIMESTAMP as time, '2018-03-01 00:00:00.000000'::TIMESTAMP AS run_time
FROM
     (
        select distinct time
        from imports.simulation_data20200520
     ) as t;


CREATE OR REPLACE VIEW air_pollution.SIM_DATA AS(
    SELECT simulation_run.ID AS SIM_R_ID, simulation_point.ID AS SIM_P_ID, pollutant_id, amount AS AMOUNT
    FROM
        imports.simulation_data20200520 AS SIM
        INNER JOIN
        air_pollution.simulation_point
        ON SIM.latitude = simulation_point.latitude AND
           SIM.longitude = simulation_point.longitude
        INNER JOIN
        air_pollution.simulation_run
        ON simulation_run.TIME = SUBSTR(SIM.time,12,20)::TIMESTAMP
);


--new prediction_pollutants
INSERT INTO air_pollution.prediction_pollutant(simulation_run_id, simulation_point_id, P1, P2, P3, P4, P5, P6, P7 ,P8 ,P9 ,P10, P11)
SELECT sim_r_id,
       sim_P_id,
       SUM(CASE WHEN pollutant_id = 1 THEN AMOUNT END) AS P1,
       SUM(CASE WHEN pollutant_id = 2 THEN AMOUNT END) AS P2,
       SUM(CASE WHEN pollutant_id = 3 THEN AMOUNT END) AS P3,
       SUM(CASE WHEN pollutant_id = 4 THEN AMOUNT END) AS P4,
       SUM(CASE WHEN pollutant_id = 5 THEN AMOUNT END) AS P5,
       SUM(CASE WHEN pollutant_id = 6 THEN AMOUNT END) AS P6,
       SUM(CASE WHEN pollutant_id = 7 THEN AMOUNT END) AS P7,
       SUM(CASE WHEN pollutant_id = 8 THEN AMOUNT END) AS P8,
       SUM(CASE WHEN pollutant_id = 9 THEN AMOUNT END) AS P9,
       SUM(CASE WHEN pollutant_id = 10 THEN AMOUNT END) AS P10,
       SUM(CASE WHEN pollutant_id = 11 THEN AMOUNT END) AS P11
FROM air_pollution.SIM_DATA
GROUP BY sim_p_id, sim_r_id;

--too slow
INSERT INTO air_pollution.prediction_pollutant(SIMULATION_RUN_ID, SIMULATION_POINT_ID)
SELECT DISTINCT SIM_R_ID, SIM_P_ID
FROM
    air_pollution.sim_data20180301;


UPDATE air_pollution.prediction_pollutant
SET P1 = amount
FROM air_pollution.SIM_DATA as SIM_DATA
WHERE SIM_DATA.pollutant_id = 1 AND
      prediction_pollutant.simulation_point_id = SIM_DATA.SIM_P_ID AND
      prediction_pollutant.simulation_run_id = SIM_DATA.SIM_R_ID;

UPDATE air_pollution.prediction_pollutant
SET P2 = amount
FROM air_pollution.SIM_DATA as SIM_DATA
WHERE SIM_DATA.pollutant_id = 2 AND
      prediction_pollutant.simulation_point_id = SIM_DATA.SIM_P_ID AND
      prediction_pollutant.simulation_run_id = SIM_DATA.SIM_R_ID;

UPDATE air_pollution.prediction_pollutant
SET P3 = amount
FROM air_pollution.SIM_DATA as SIM_DATA
WHERE SIM_DATA.pollutant_id = 3 AND
      prediction_pollutant.simulation_point_id = SIM_DATA.SIM_P_ID AND
      prediction_pollutant.simulation_run_id = SIM_DATA.SIM_R_ID;

UPDATE air_pollution.prediction_pollutant
SET P4 = amount
FROM air_pollution.SIM_DATA as SIM_DATA
WHERE SIM_DATA.pollutant_id = 4 AND
      prediction_pollutant.simulation_point_id = SIM_DATA.SIM_P_ID AND
      prediction_pollutant.simulation_run_id = SIM_DATA.SIM_R_ID;

UPDATE air_pollution.prediction_pollutant
SET P5 = amount
FROM air_pollution.SIM_DATA as SIM_DATA
WHERE SIM_DATA.pollutant_id = 5 AND
      prediction_pollutant.simulation_point_id = SIM_DATA.SIM_P_ID AND
      prediction_pollutant.simulation_run_id = SIM_DATA.SIM_R_ID;

UPDATE air_pollution.prediction_pollutant
SET P6 = amount
FROM air_pollution.SIM_DATA as SIM_DATA
WHERE SIM_DATA.pollutant_id = 6 AND
      prediction_pollutant.simulation_point_id = SIM_DATA.SIM_P_ID AND
      prediction_pollutant.simulation_run_id = SIM_DATA.SIM_R_ID;

UPDATE air_pollution.prediction_pollutant
SET P7 = amount
FROM air_pollution.SIM_DATA as SIM_DATA
WHERE SIM_DATA.pollutant_id = 7 AND
      prediction_pollutant.simulation_point_id = SIM_DATA.SIM_P_ID AND
      prediction_pollutant.simulation_run_id = SIM_DATA.SIM_R_ID;

UPDATE air_pollution.prediction_pollutant
SET P8 = amount
FROM air_pollution.SIM_DATA as SIM_DATA
WHERE SIM_DATA.pollutant_id = 8 AND
      prediction_pollutant.simulation_point_id = SIM_DATA.SIM_P_ID AND
      prediction_pollutant.simulation_run_id = SIM_DATA.SIM_R_ID;

UPDATE air_pollution.prediction_pollutant
SET P9 = amount
FROM air_pollution.SIM_DATA as SIM_DATA
WHERE SIM_DATA.pollutant_id = 9 AND
      prediction_pollutant.simulation_point_id = SIM_DATA.SIM_P_ID AND
      prediction_pollutant.simulation_run_id = SIM_DATA.SIM_R_ID;

UPDATE air_pollution.prediction_pollutant
SET P10 = amount
FROM air_pollution.SIM_DATA as SIM_DATA
WHERE SIM_DATA.pollutant_id = 10 AND
      prediction_pollutant.simulation_point_id = SIM_DATA.SIM_P_ID AND
      prediction_pollutant.simulation_run_id = SIM_DATA.SIM_R_ID;

UPDATE air_pollution.prediction_pollutant
SET P11 = amount
FROM air_pollution.SIM_DATA as SIM_DATA
WHERE SIM_DATA.pollutant_id = 11 AND
      prediction_pollutant.simulation_point_id = SIM_DATA.SIM_P_ID AND
      prediction_pollutant.simulation_run_id = SIM_DATA.SIM_R_ID;
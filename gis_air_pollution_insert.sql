INSERT INTO air_pollution.POLLUTANT(ID,POLLUTANT,DESCRIPTION,NAME)
SELECT id, pollutant, description, name_mv FROM public.postgres_public_pollutant;


INSERT INTO air_pollution.POLLUTANT
VALUES (12,'O3','O3','O3'),(13,'CO2','CO2','CO2');


INSERT INTO air_pollution.sensor(ENDPOINT_NAME, name, longitude, latitude)
SELECT c1, station_name, longitude, latitude FROM public.postgres_public_sensors_mv;


INSERT INTO air_pollution.POLLUTANT_MEASUREMENT(pollutant_id, sensor_id, amount, time)
SELECT pollutant.ID, sensor.id, a.data, a.datetime
FROM public.postgres_public_measurements_2020 as a,
     air_pollution.POLLUTANT as pollutant,
     air_pollution.sensor
WHERE (pollutant.DESCRIPTION = a.pollutant or pollutant.name = a.pollutant or pollutant.pollutant = a.pollutant) and a.station = sensor.name;


INSERT INTO air_pollution.Simulation_Point(longitude, latitude)
SELECT DISTINCT longitude, latitude
FROM public.select___from_sim20200103_limit_10000;


INSERT INTO air_pollution.Simulation_Run(id, time, RUN_TIME)
SELECT ID, model.date + INTERVAL '1H'*model.hour,  model.run_date
FROM public.postgres_public_model_output as model;

INSERT INTO air_pollution.pollution_source(longitude, latitude, gnfr_aggregated_sectors, bix_no2_kt, nmvoc_kt, sox_so2_kt, nh3_kt, pm25_kt, pm10_kt, bc_kt, co_kt, pb_kt, cd_kt, hg_kt, pcdd_pcdf_dioxins_furans_gi_teq, pahs_t, hcb_kg, pcbs_kg, square_area, point_geo, total_are_km2, urban_area_km2, point_geom)
SELECT *
FROM public.postgres_public_gridded_emissions_mk_2015;

--first i input all predictions with null values for all pollutants,
--and then i add all the pollutants


INSERT INTO air_pollution.PREDICTION_POLLUTANT(SIMULATION_POINT_ID,SIMULATION_RUN_ID)
SELECT DISTINCT sim_p.id, sim.id
FROM public.select___from_sim20200103_limit_10000 as sim,
     air_pollution.Simulation_Point as sim_p,
     public.postgres_public_model_output as model
WHERE sim.id = model.id
  and sim_p.latitude = sim.latitude
  and sim_p.longitude = sim.longitude;

UPDATE air_pollution.PREDICTION_POLLUTANT
SET P1 = sim.data
FROM public.select___from_sim20200103_limit_10000 as sim,
     air_pollution.Simulation_Point as sim_p,
     air_pollution.simulation_run as sim_r
WHERE sim.pollutant_id = 1
  and simulation_point_id = sim_p.id
  and simulation_run_id = sim_r.id
  and sim_p.latitude = sim.latitude
  and sim_p.longitude = sim.longitude;

UPDATE air_pollution.PREDICTION_POLLUTANT
SET P2 = sim.data
FROM public.select___from_sim20200103_limit_10000 as sim,
     air_pollution.Simulation_Point as sim_p,
     air_pollution.simulation_run as sim_r
WHERE sim.pollutant_id = 2
  and simulation_point_id = sim_p.id
  and simulation_run_id = sim_r.id
  and sim_p.latitude = sim.latitude
  and sim_p.longitude = sim.longitude;

UPDATE air_pollution.PREDICTION_POLLUTANT
SET P3 = sim.data
FROM public.select___from_sim20200103_limit_10000 as sim,
     air_pollution.Simulation_Point as sim_p,
     air_pollution.simulation_run as sim_r
WHERE sim.pollutant_id = 3
  and simulation_point_id = sim_p.id
  and simulation_run_id = sim_r.id
  and sim_p.latitude = sim.latitude
  and sim_p.longitude = sim.longitude;

UPDATE air_pollution.PREDICTION_POLLUTANT
SET P4 = sim.data
FROM public.select___from_sim20200103_limit_10000 as sim,
     air_pollution.Simulation_Point as sim_p,
     air_pollution.simulation_run as sim_r
WHERE sim.pollutant_id = 4
  and simulation_point_id = sim_p.id
  and simulation_run_id = sim_r.id
  and sim_p.latitude = sim.latitude
  and sim_p.longitude = sim.longitude;

UPDATE air_pollution.PREDICTION_POLLUTANT
SET P5 = sim.data
FROM public.select___from_sim20200103_limit_10000 as sim,
     air_pollution.Simulation_Point as sim_p,
     air_pollution.simulation_run as sim_r
WHERE sim.pollutant_id = 5
  and simulation_point_id = sim_p.id
  and simulation_run_id = sim_r.id
  and sim_p.latitude = sim.latitude
  and sim_p.longitude = sim.longitude;

UPDATE air_pollution.PREDICTION_POLLUTANT
SET P5 = sim.data
FROM public.select___from_sim20200103_limit_10000 as sim,
     air_pollution.Simulation_Point as sim_p,
     air_pollution.simulation_run as sim_r
WHERE sim.pollutant_id = 5
  and simulation_point_id = sim_p.id
  and simulation_run_id = sim_r.id
  and sim_p.latitude = sim.latitude
  and sim_p.longitude = sim.longitude;

UPDATE air_pollution.PREDICTION_POLLUTANT
SET P6 = sim.data
FROM public.select___from_sim20200103_limit_10000 as sim,
     air_pollution.Simulation_Point as sim_p,
     air_pollution.simulation_run as sim_r
WHERE sim.pollutant_id = 6
  and simulation_point_id = sim_p.id
  and simulation_run_id = sim_r.id
  and sim_p.latitude = sim.latitude
  and sim_p.longitude = sim.longitude;

UPDATE air_pollution.PREDICTION_POLLUTANT
SET P7 = sim.data
FROM public.select___from_sim20200103_limit_10000 as sim,
     air_pollution.Simulation_Point as sim_p,
     air_pollution.simulation_run as sim_r
WHERE sim.pollutant_id = 7
  and simulation_point_id = sim_p.id
  and simulation_run_id = sim_r.id
  and sim_p.latitude = sim.latitude
  and sim_p.longitude = sim.longitude;

UPDATE air_pollution.PREDICTION_POLLUTANT
SET P8 = sim.data
FROM public.select___from_sim20200103_limit_10000 as sim,
     air_pollution.Simulation_Point as sim_p,
     air_pollution.simulation_run as sim_r
WHERE sim.pollutant_id = 8
  and simulation_point_id = sim_p.id
  and simulation_run_id = sim_r.id
  and sim_p.latitude = sim.latitude
  and sim_p.longitude = sim.longitude;

UPDATE air_pollution.PREDICTION_POLLUTANT
SET P9 = sim.data
FROM public.select___from_sim20200103_limit_10000 as sim,
     air_pollution.Simulation_Point as sim_p,
     air_pollution.simulation_run as sim_r
WHERE sim.pollutant_id = 9
  and simulation_point_id = sim_p.id
  and simulation_run_id = sim_r.id
  and sim_p.latitude = sim.latitude
  and sim_p.longitude = sim.longitude;

UPDATE air_pollution.PREDICTION_POLLUTANT
SET P10 = sim.data
FROM public.select___from_sim20200103_limit_10000 as sim,
     air_pollution.Simulation_Point as sim_p,
     air_pollution.simulation_run as sim_r
WHERE sim.pollutant_id = 10
  and simulation_point_id = sim_p.id
  and simulation_run_id = sim_r.id
  and sim_p.latitude = sim.latitude
  and sim_p.longitude = sim.longitude;

UPDATE air_pollution.PREDICTION_POLLUTANT
SET P8 = sim.data
FROM public.select___from_sim20200103_limit_10000 as sim,
     air_pollution.Simulation_Point as sim_p,
     air_pollution.simulation_run as sim_r
WHERE sim.pollutant_id = 8
  and simulation_point_id = sim_p.id
  and simulation_run_id = sim_r.id
  and sim_p.latitude = sim.latitude
  and sim_p.longitude = sim.longitude;

UPDATE air_pollution.PREDICTION_POLLUTANT
SET P11 = sim.data
FROM public.select___from_sim20200103_limit_10000 as sim,
     air_pollution.Simulation_Point as sim_p,
     air_pollution.simulation_run as sim_r
WHERE sim.pollutant_id = 11
  and simulation_point_id = sim_p.id
  and simulation_run_id = sim_r.id
  and sim_p.latitude = sim.latitude
  and sim_p.longitude = sim.longitude;
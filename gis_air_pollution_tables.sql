drop schema air_pollution cascade;
--drop schema imports cascade;
CREATE SCHEMA air_pollution;
--CREATE SCHEMA imports;
CREATE EXTENSION postgis SCHEMA air_pollution;
CREATE TABLE air_pollution.Simulation_Run(
    ID SERIAL PRIMARY KEY,
    TIME TIMESTAMP NOT NULL,
    RUN_TIME TIMESTAMP NOT NULL
);

CREATE TABLE air_pollution.pollution_source
(
    id  serial primary key,
    longitude                       double precision,
    latitude                        double precision,
    gnfr_aggregated_sectors         text,
    bix_no2_kt                      double precision,
    nmvoc_kt                        double precision,
    sox_so2_kt                      double precision,
    nh3_kt                          double precision,
    pm25_kt                         double precision,
    pm10_kt                         double precision,
    bc_kt                           double precision,
    co_kt                           double precision,
    pb_kt                           double precision,
    cd_kt                           double precision,
    hg_kt                           double precision,
    pcdd_pcdf_dioxins_furans_gi_teq double precision,
    pahs_t                          double precision,
    hcb_kg                          double precision,
    pcbs_kg                         double precision,
    square_area                     TEXT,
    point_geo                       text,
    total_are_km2                   double precision,
    urban_area_km2                  double precision,
    point_geom                      text
);

CREATE TABLE air_pollution.Simulation_Point(
    ID SERIAL PRIMARY KEY,
    LONGITUDE DOUBLE PRECISION NOT NULL,
    LATITUDE DOUBLE PRECISION NOT NULL,
    UNIQUE (LONGITUDE, LATITUDE)
);

CREATE TABLE air_pollution.Pollution_Source_Distance(
    SIMULATION_POINT_ID INTEGER REFERENCES air_pollution.Simulation_Point(ID),
    POLLUTION_SOURCE_ID INTEGER REFERENCES air_pollution.Pollution_Source(ID),
    DISTANCE DOUBLE PRECISION,
    PRIMARY KEY (SIMULATION_POINT_ID, POLLUTION_SOURCE_ID)
);

CREATE TABLE air_pollution.Sensor(
    ID SERIAL PRIMARY KEY,
    ENDPOINT_NAME TEXT,
    NAME TEXT UNIQUE,
    LONGITUDE FLOAT NOT NULL,
    LATITUDE FLOAT NOT NULL
);

CREATE TABLE air_pollution.Sensor_Distance(
    SIMULATION_POINT_ID INTEGER REFERENCES air_pollution.Simulation_Point(ID) ,
    SENSOR_ID INTEGER REFERENCES air_pollution.Sensor(ID),
    DISTANCE DOUBLE PRECISION,
    PRIMARY KEY (SIMULATION_POINT_ID, SENSOR_ID)
);

CREATE TABLE air_pollution.POLLUTANT(
    ID INTEGER PRIMARY KEY,
    POLLUTANT TEXT,
    DESCRIPTION TEXT,
    NAME TEXT
);

CREATE TABLE air_pollution.POLLUTANT_MEASUREMENT(
    ID SERIAL PRIMARY KEY,
    POLLUTANT_ID INTEGER REFERENCES air_pollution.POLLUTANT(ID),
    SENSOR_ID INTEGER REFERENCES air_pollution.Sensor(ID),
    AMOUNT DOUBLE PRECISION,
    TIME TIMESTAMP
);

CREATE TABLE air_pollution.PREDICTION_POLLUTANT(
    ID SERIAL PRIMARY KEY,
    SIMULATION_RUN_ID INTEGER REFERENCES air_pollution.Simulation_Run(ID),
    SIMULATION_POINT_ID INTEGER REFERENCES air_pollution.simulation_point(ID),
    p1 DOUBLE PRECISION,
    p2 DOUBLE PRECISION,
    p3 DOUBLE PRECISION,
    p4 DOUBLE PRECISION,
    p5 DOUBLE PRECISION,
    p6 DOUBLE PRECISION,
    p7 DOUBLE PRECISION,
    p8 DOUBLE PRECISION,
    p9 DOUBLE PRECISION,
    p10 DOUBLE PRECISION,
    p11 DOUBLE PRECISION,
    UNIQUE (SIMULATION_RUN_ID, SIMULATION_POINT_ID)
);
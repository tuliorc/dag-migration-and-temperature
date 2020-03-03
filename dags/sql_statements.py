CREATE_STAGING_AIRPORTS_REDSHIFT = ("""
    CREATE TABLE IF NOT EXISTS public.df_airports (
    ident VARCHAR(100) NOT NULL,
    type VARCHAR(100) NOT NULL,
    name VARCHAR(100),
    elevation_ft DECIMAL(16,2),
    iso_region VARCHAR(100),
    municipality VARCHAR(100),
    gps_code VARCHAR(100),
    local_code VARCHAR(100),
    coordinates VARCHAR(100),
    PRIMARY KEY(ident))
    DISTSTYLE ALL;
    """)

CREATE_STAGING_TEMPERATURES_REDSHIFT = ("""
    CREATE TABLE IF NOT EXISTS public.df_temperatures (
    dt VARCHAR(100) NOT NULL,
    AverageTemperature DECIMAL(16,2),
    AverageTemperatureUncertainty DECIMAL(16,2),
    City VARCHAR(100),
    Country VARCHAR(100),
    Latitude VARCHAR(100),
    Longitude VARCHAR(100),
    PRIMARY KEY(dt, City))
    DISTSTYLE ALL;
    """)

CREATE_STAGING_DEMOGRAPHICS_REDSHIFT = ("""
    CREATE TABLE IF NOT EXISTS public.df_demographics (
    City VARCHAR(100) NOT NULL,
    State VARCHAR(100) NOT NULL,
    Median_Age DECIMAL(16, 2),
    Male_Population DECIMAL(16, 2),
    Female_Population DECIMAL(16, 2),
    Total_Population DECIMAL(16, 2),
    Foreign_Born DECIMAL(16, 2),
    State_Code VARCHAR(100) NOT NULL,
    Race VARCHAR(100) NOT NULL,
    Count INTEGER NOT NULL,
    PRIMARY KEY(City, Race))
    DISTSTYLE ALL;
    """)

CREATE_STAGING_MIGRATION_REDSHIFT = ("""
	CREATE TABLE IF NOT EXISTS public.staging_migration (
	i94_year INTEGER NOT NULL,
	i94_month INTEGER,
	city_origin INTEGER NOT NULL,
	city_destination INTEGER NOT NULL,
	airport_code VARCHAR(100),
	arrival_date INTEGER,
	departure_date INTEGER,
	i94_visa INTEGER,
	visa_type VARCHAR(100),
	year_birth INTEGER)
	DISTSTYLE ALL;
	""")

CREATE_DIM_WEATHER = ("""
    CREATE TABLE public.dim_weather AS
     SELECT year(dt) as year,
            month(dt) as month,
            city,
            AVG(AverageTemperature) as average_monthly_temperature
      FROM public.df_temperatures
      GROUP BY year, month
      """)

CREATE_DIM_CITIES = ("""
    CREATE TABLE public.dim_cities AS
     SELECT
        City,
        State Code,
        Total Population,
        Median Age
        Foreign-Born,
        SUM(Count)
      FROM public.df_demographics
      GROUP BY City
      """)

CREATE_FACT_MIGRATION = ("""
    CREATE TABLE public.fact_migration AS
     SELECT
         year(arrival_date) as year_arrival,
         month(arrival_date) as month_arrival,
         day(arrival_date) as day_arrival,
         city_origin as city_origin,
         city_destination as city_destination,
         airport_code as airport_code,
         visa_type as visa type
     FROM public.df_migration
      """)

CREATE_STATS_MIGRATION = ("""
    CREATE TABLE public.stats_migration AS
     SELECT
         year(mig.arrival_date) as year_arrival,
         month(mig.arrival_date) as month_arrival,
         temp.average_monthly_temperature as average_monthly_temperature,
         mig.city_destination as city,
         (SELECT COUNT(*) FROM public.fact_migration \
            WHERE city_destination = city) AS amount_immigrants,
         (SELECT COUNT(*) FROM public.fact_migration \
            WHERE city_origin = city) AS amount_emigrants
     FROM
        public.dim_migration mig LEFT JOIN \
            public.dim_temperatures temp ON temp.city_id = mig.city_id
     GROUP BY
        mig.year_arrival,
        mig.month_arrival
      """)

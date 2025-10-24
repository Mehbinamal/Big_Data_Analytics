-- Load the weather dataset (skip header)
weather = LOAD 'DailyDelhiClimateTrain.csv' USING PigStorage(',') 
           AS (date:chararray, meantemp:double, humidity:double, wind_speed:double, meanpressure:double);

-- Extract year from date
weather_with_year = FOREACH weather GENERATE 
                        SUBSTRING(date, 0, 4) AS year, 
                        meantemp;

-- Group data by year
grouped_by_year = GROUP weather_with_year BY year;

-- Find maximum temperature per year
max_temp_by_year = FOREACH grouped_by_year GENERATE
                      group AS year,
                      MAX(weather_with_year.meantemp) AS max_temperature;

-- Store the result
STORE max_temp_by_year INTO 'output_max_temp' USING PigStorage(',');

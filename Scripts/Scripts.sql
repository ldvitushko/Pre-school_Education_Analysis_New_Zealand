CREATE DATABASE final_project;
CREATE SCHEMA IF NOT EXISTS fp;

-- Create dimension table with quarters (the minimum time division by which you we find data on fertility or living standards)
CREATE TABLE fp.dim_times (
	time_id 				VARCHAR(6) 	PRIMARY KEY,
	start_month_name 		VARCHAR(9) 	NOT NULL,
	start_month_id 			INTEGER 	NOT NULL,
	end_month_name 			VARCHAR(9) 	NOT NULL,
	end_month_id 			INTEGER 	NOT NULL,
	calendar_quarter_number INTEGER 	NOT NULL,
	calendar_year 			INTEGER 	NOT NULL
);

-- Function to fill the data in the dim_times table
DROP FUNCTION IF EXISTS fp.fill_times(INTEGER, INTEGER);

CREATE OR REPLACE FUNCTION fp.fill_times(start_year INTEGER, end_year INTEGER)
RETURNS TABLE (	r_time_id fp.dim_times.time_id%TYPE,
				r_start_month_name fp.dim_times.start_month_name%TYPE,
				r_start_month_id fp.dim_times.start_month_id%TYPE,
				r_end_month_name fp.dim_times.end_month_name%TYPE,
				r_end_month_id fp.dim_times.end_month_id%TYPE,
				r_calendar_quarter_number fp.dim_times.calendar_quarter_number%TYPE,
				r_calendar_year fp.dim_times.calendar_year%TYPE
				)
LANGUAGE plpgsql

AS $$
DECLARE 
	i INTEGER;
	-- For convenience, let's create an array of quarters
	quart TEXT ARRAY := ARRAY['Q1', 'Q2', 'Q3', 'Q4'];
	x TEXT;
BEGIN
	-- Some sanity checks... 
	IF start_year IN (SELECT t.calendar_year FROM fp.dim_times t) 
		OR end_year IN (SELECT t.calendar_year FROM fp.dim_times t) 
	THEN 
    RAISE EXCEPTION 'Such year already exists. Please, input another date.';
    END IF;
   
   	IF start_year > end_year THEN
    RAISE EXCEPTION 'Please, input correct date.';
    END IF;
	
    -- Go through the years
	FOR i IN start_year..end_year LOOP
		-- Go through the quarters in the years
		FOREACH x IN ARRAY quart
		LOOP
			RETURN QUERY
			INSERT INTO fp.dim_times (time_id, start_month_name, start_month_id, end_month_name, end_month_id, calendar_quarter_number, calendar_year)	
			SELECT 
				i::TEXT || x::TEXT,
				CASE  
					WHEN x=quart[1] THEN 'January'
					WHEN x=quart[2] THEN 'April'
					WHEN x=quart[3] THEN 'July'
					WHEN x=quart[4] THEN 'October'
				END,
				array_position(quart, x)*3-2,
				CASE  
					WHEN x=quart[1] THEN 'March'
					WHEN x=quart[2] THEN 'June'
					WHEN x=quart[3] THEN 'September'
					WHEN x=quart[4] THEN 'December'
				END,
				array_position(quart, x)*3,
				array_position(quart, x),
				i
			RETURNING *;
		END LOOP;
	END LOOP;
	RETURN;
END;
$$

-- Fill the data in the dim_times table from 1991 to 2020
SELECT fp.fill_times(1991, 2020);

-- Standart ISO/IEC 5218 (genders) 
-- 0 = Not known;
-- 1 = Male;
-- 2 = Female;
-- 9 = Not applicable.

-- Table with genders according to the standard
CREATE TABLE fp.dim_gender (
	gender_id SMALLINT PRIMARY KEY,
	sex VARCHAR(255) UNIQUE NOT NULL
);

-- Fill dim_gender table according to the standard
INSERT INTO fp.dim_gender (gender_id, sex) VALUES 
	(0, 'Not known'),
	(1, 'Male'),
	(2, 'Female'),
	(9, 'Not applicable')
RETURNING *;

-- Table containing quarterly fertility data for selected cities
CREATE TABLE fp.births (
	time_id VARCHAR(6) NOT NULL,
	city VARCHAR(255),
	births_count INTEGER,
	country VARCHAR(255) DEFAULT 'New Zealand'
);

-- Auxiliary table for correct subsequent reading of data from a CSV file
CREATE TABLE fp.births_foo (
	time_id VARCHAR(6) NOT NULL
);

-- Temporary table to read the names of all columns from the CSV file, and not write them manually
CREATE TABLE fp.foo(
	x TEXT
);

-- Copy data into temporary table in one column
COPY fp.foo
FROM 'D:\TrainingDE\Project\Births.csv';

-- Function to add columns into births_foo table 
-- The input is an array of cities, the names of which we will get from the temporary table fp.foo
DROP FUNCTION IF EXISTS fp.add_birth_cities(TEXT[]);

CREATE OR REPLACE FUNCTION fp.add_birth_cities(cities TEXT[])
RETURNS TEXT
LANGUAGE plpgsql

AS $$
DECLARE 
	v_city TEXT;
	counter INT := 0;
BEGIN
FOREACH v_city IN ARRAY cities
	LOOP
	IF UPPER(v_city) NOT IN (
		SELECT UPPER(column_name)
		FROM information_schema.columns WHERE table_name='births_foo'
	) THEN
		BEGIN 
			EXECUTE 'ALTER TABLE fp.births_foo ADD "'|| v_city || '" INTEGER default NULL;'; -- Use ", because city name may contain spaces
			counter:=counter+1;
		END;
    END IF;
	END LOOP;
RETURN counter || ' column(s) has been successfully added!';
END;
$$

-- Create columns
SELECT fp.add_birth_cities(string_to_array((SELECT x FROM fp.foo ORDER BY 1 DESC LIMIT 1), ','));

DROP TABLE fp.foo;

COPY fp.births_foo
FROM 'D:\TrainingDE\Project\Births.csv'
DELIMITER ','
CSV HEADER;

UPDATE 
	fp.births_foo
SET 
	"Auckland (to 2010)"=b."Auckland (from 2011)"
FROM 
	fp.births_foo b
WHERE fp.births_foo.time_id=b.time_id
AND fp.births_foo."Auckland (to 2010)" IS NULL;

ALTER TABLE fp.births_foo
RENAME COLUMN "Auckland (to 2010)" TO "Auckland";

ALTER TABLE fp.births_foo
DROP COLUMN "Auckland (from 2011)";

-- Unpivot births
INSERT INTO fp.births (time_id, city, births_count)
SELECT 
	time_id,
	x."city",
	x.births::INTEGER
-- to_jsonb() converts the whole row into a JSON value using the column names as keys and jsonb_each_text() is the unnesting of that JSON value.
FROM fp.births_foo b2, jsonb_each_text(to_jsonb(b2)) as x("city",births)
WHERE UPPER(x.city) != 'TIME_ID'
ORDER BY b2.time_id;

DROP TABLE fp.births_foo;

ALTER TABLE fp.births ADD CONSTRAINT timefk FOREIGN KEY (time_id) REFERENCES fp.dim_times (time_id) ON UPDATE CASCADE ON DELETE RESTRICT;

CREATE TABLE fp.cities_population (
	city VARCHAR(255),
	city_ascii VARCHAR(255),
	latitude NUMERIC(18,4),
	longitude NUMERIC(18,4),
	country VARCHAR(255),
	iso2 VARCHAR(2),
	iso3 VARCHAR(3),
	admin_name VARCHAR(255),
	capital VARCHAR(255),
	population BIGINT,
	id BIGINT PRIMARY KEY
);

COPY fp.cities_population
FROM 'D:\TrainingDE\Project\worldcities.csv'
DELIMITER ';'
CSV HEADER;

CREATE TABLE fp.dim_cities (
	id BIGINT PRIMARY KEY,
	city VARCHAR(255),
	city_ascii VARCHAR(255),
	city_country VARCHAR(255) GENERATED ALWAYS AS (city_ascii || ', ' || country) STORED NOT NULL,
	latitude NUMERIC(18,4),
	longitude NUMERIC(18,4),
	country VARCHAR(255),
	iso2 VARCHAR(2),
	iso3 VARCHAR(3),
	admin_name VARCHAR(255),
	capital VARCHAR(255)
);

INSERT INTO fp.dim_cities (id, city, city_ascii, latitude, longitude, country, iso2, iso3, admin_name, capital)
SELECT 
	cp.id,
	cp.city,
	cp.city_ascii,
	cp.latitude,
	cp.longitude,
	cp.country,
	cp.iso2,
	cp.iso3,
	cp.admin_name,
	cp.capital 
FROM fp.cities_population cp;


UPDATE 
	fp.births
SET 
	city=c.id
FROM 
	fp.dim_cities c
WHERE fp.births.city=c.city_ascii
AND fp.births.country=c.country;

ALTER TABLE fp.births
RENAME COLUMN city TO city_id;

ALTER TABLE fp.births
ALTER COLUMN city_id TYPE BIGINT
USING city_id::BIGINT;

ALTER TABLE fp.births
DROP COLUMN country;

ALTER TABLE fp.births ADD CONSTRAINT cityfk FOREIGN KEY (city_id) REFERENCES fp.dim_cities (id) ON UPDATE CASCADE ON DELETE RESTRICT;
ALTER TABLE fp.births ADD CONSTRAINT birthspk PRIMARY KEY (time_id, city_id);

CREATE TABLE fp.earnings (
	time_id VARCHAR(6) NOT NULL,
	city VARCHAR(255),
	sex VARCHAR(255),
	ordinary_time_weekly NUMERIC(18,2),
	overtime_weekly NUMERIC(18,2)
);

/* Import data from CSV */
COPY fp.earnings(time_id, city, sex, ordinary_time_weekly, overtime_weekly)
FROM 'D:\TrainingDE\Project\Earnings.csv'
DELIMITER ';'
CSV HEADER;

--Preparing data in the table for communication
UPDATE 
	fp.earnings
SET 
	sex=g.gender_id
FROM 
	fp.dim_gender g 
WHERE UPPER(fp.earnings.sex)=UPPER(g.sex);

ALTER TABLE fp.earnings
RENAME COLUMN sex TO gender_id;

ALTER TABLE fp.earnings
ALTER COLUMN gender_id TYPE SMALLINT
USING gender_id::SMALLINT;

ALTER TABLE fp.earnings ADD CONSTRAINT sexfk FOREIGN KEY (gender_id) REFERENCES fp.dim_gender (gender_id) ON UPDATE CASCADE ON DELETE RESTRICT;
ALTER TABLE fp.earnings ADD CONSTRAINT timefk FOREIGN KEY (time_id) REFERENCES fp.dim_times (time_id) ON UPDATE CASCADE ON DELETE RESTRICT;

UPDATE 
	fp.earnings
SET 
	city=c.id
FROM 
	fp.dim_cities c
WHERE fp.earnings.city=c.city_ascii
AND UPPER(c.country) = 'NEW ZEALAND';

ALTER TABLE fp.earnings
RENAME COLUMN city TO city_id;

ALTER TABLE fp.earnings
ALTER COLUMN city_id TYPE BIGINT
USING city_id::BIGINT;

ALTER TABLE fp.earnings ADD CONSTRAINT cityfk FOREIGN KEY (city_id) REFERENCES fp.dim_cities (id) ON UPDATE CASCADE ON DELETE RESTRICT;
ALTER TABLE fp.earnings ADD CONSTRAINT earningspk PRIMARY KEY (time_id, city_id, gender_id);


CREATE TABLE fp.population (
	time_id VARCHAR(6) NOT NULL DEFAULT '2020Q4',
	city_id BIGINT,
	population BIGINT
);

INSERT INTO fp.population (city_id, population)
SELECT 
	cp.id,
	cp.population
FROM fp.cities_population cp;

ALTER TABLE fp.population ADD CONSTRAINT cityfk FOREIGN KEY (city_id) REFERENCES fp.dim_cities (id) ON UPDATE CASCADE ON DELETE RESTRICT;
ALTER TABLE fp.population ADD CONSTRAINT timefk FOREIGN KEY (time_id) REFERENCES fp.dim_times (time_id) ON UPDATE CASCADE ON DELETE RESTRICT;
ALTER TABLE fp.population ADD CONSTRAINT populationpk PRIMARY KEY (time_id, city_id);


DROP TABLE fp.cities_population;

-- At this point, I realized that I can export tables with data from a CSV connection, so there will be no script to create a table and import data. 
-- But the resulting table has to be processed for further work with it.

-- First of all, the table contains columns that contradict each other.
-- The total number of children enrolled as at the "Roll_Date" exceeded the sum of children by ethnicity
-- The data from these columns have no practical meaning for us and we cannot verify their reliability, therefore, we will remove the questionable columns.

ALTER TABLE fp.kindergartens DROP COLUMN "Roll_Date";
ALTER TABLE fp.kindergartens DROP COLUMN "Total";
ALTER TABLE fp.kindergartens DROP COLUMN "European";
ALTER TABLE fp.kindergartens DROP COLUMN "Maori";
ALTER TABLE fp.kindergartens DROP COLUMN "Pacific";
ALTER TABLE fp.kindergartens DROP COLUMN "Asian";
ALTER TABLE fp.kindergartens DROP COLUMN "Other";

-- We cannot turn varchar into integer if there is "", so we use NULLIF and insert NULL in this cases

-- Function to replace "" to NULL in any table:

DROP FUNCTION IF EXISTS fp.update_nulls(VARCHAR(255));

CREATE OR REPLACE FUNCTION fp.update_nulls(tab_name VARCHAR(255))
RETURNS TEXT
LANGUAGE plpgsql

AS $$
DECLARE 
	v_column TEXT;
	table_columns TEXT ARRAY := string_to_array((SELECT string_agg(COLUMN_NAME, ',') FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = tab_name), ',');
	tmpSQL TEXT;
BEGIN
	FOREACH v_column IN ARRAY table_columns
	LOOP
	 	tmpSQL := 'UPDATE fp.'|| tab_name ||' SET "' || v_column || '"=NULLIF("' || v_column || '", '''')';
	    EXECUTE tmpSQL;
	END LOOP;
RETURN 'Cells updated!';
END;
$$

-- Try it
SELECT fp.update_nulls('kindergartens');

-- Change some types of data
ALTER TABLE fp.kindergartens ALTER COLUMN "ECE_Id" TYPE INTEGER USING "ECE_Id":INTEGER;
-- We will not change the data type for the phone, since some numbers start with zero, and when cast to the BIGINT type 
-- (some numbers are very long and would not fit into INTEGER), zero will be lost
-- Also, for future use, it may be inconvenient to store management_telephones with spaces and dashes in the database, so we will delete them
UPDATE fp.kindergartens SET "Management_Telephone" = REPLACE("Management_Telephone", '-', '');
UPDATE fp.kindergartens SET "Management_Telephone" = REPLACE("Management_Telephone", ' ', '');
-- AND fax
UPDATE fp.kindergartens SET "Management_Fax" = REPLACE("Management_Fax", ' ', '');
UPDATE fp.kindergartens SET "Management_Fax" = REPLACE("Management_Fax", '-', '');

UPDATE 
	fp.kindergartens
SET 
	"Add1_City"=c.id
FROM 
	fp.dim_cities c
WHERE UPPER(fp.kindergartens."Add1_City")=UPPER(c.city_ascii);

ALTER TABLE fp.kindergartens
RENAME COLUMN "Add1_City" TO "city_id";

ALTER TABLE fp.kindergartens
ALTER COLUMN "city_id" TYPE BIGINT
USING "city_id"::BIGINT;

ALTER TABLE fp.kindergartens ADD CONSTRAINT cityfk FOREIGN KEY ("city_id") REFERENCES fp.dim_cities (id) ON UPDATE CASCADE ON DELETE RESTRICT;
ALTER TABLE fp.kindergartens ADD CONSTRAINT ece_idpk PRIMARY KEY ("ECE_Id");

-- Change the data type of the column so that it is convenient to analyze them and in the future it would be impossible to educate 2/3 of the child, "" quotes do not bother us
ALTER TABLE fp.kindergartens ALTER COLUMN "All_Children" TYPE INTEGER USING "All_Children"::INTEGER;
ALTER TABLE fp.kindergartens ALTER COLUMN "Under_2s" TYPE INTEGER USING "Under_2s"::INTEGER;
ALTER TABLE fp.kindergartens ALTER COLUMN "Age_0" TYPE INTEGER USING "Age_0"::INTEGER;
ALTER TABLE fp.kindergartens ALTER COLUMN "Age_1" TYPE INTEGER USING "Age_1"::INTEGER;
ALTER TABLE fp.kindergartens ALTER COLUMN "Age_2" TYPE INTEGER USING "Age_2"::INTEGER;
ALTER TABLE fp.kindergartens ALTER COLUMN "Age_3" TYPE INTEGER USING "Age_3"::INTEGER;
ALTER TABLE fp.kindergartens ALTER COLUMN "Age_4" TYPE INTEGER USING "Age_4"::INTEGER;
ALTER TABLE fp.kindergartens ALTER COLUMN "Age_5" TYPE INTEGER USING "Age_5"::INTEGER; 

--  Birth statistics for different cities in New Zealand

CREATE OR REPLACE VIEW fp.births_analyse
AS
SELECT
	tab.city,
	tab.country,
	tab.current_births,
	tab.prev_year1,
	tab.prev_year2,
	tab.prev_year3,
	tab.prev_year4,
	tab.prev_year5
FROM (
	SELECT 
		dt.calendar_year,
		dc.city,
		dc.country,
		sum(b.births_count) current_births, -- относительный прирост рождаемости
		round(lag(sum(b.births_count), 0) OVER w*100/(lag(sum(b.births_count), 1) OVER w)::NUMERIC(18,2)-100, 2)||'%' AS prev_year1,
		round(lag(sum(b.births_count), 1) OVER w*100/(lag(sum(b.births_count), 2) OVER w)::NUMERIC(18,2)-100, 2)||'%' AS prev_year2,
		round(lag(sum(b.births_count), 2) OVER w*100/(lag(sum(b.births_count), 3) OVER w)::NUMERIC(18,2)-100, 2)||'%' AS prev_year3,
		round(lag(sum(b.births_count), 3) OVER w*100/(lag(sum(b.births_count), 4) OVER w)::NUMERIC(18,2)-100, 2)||'%' AS prev_year4,
		round(lag(sum(b.births_count), 4) OVER w*100/(lag(sum(b.births_count), 5) OVER w)::NUMERIC(18,2)-100, 2)||'%' AS prev_year5
	FROM fp.births b 
	INNER JOIN fp.dim_times dt ON dt.time_id = b.time_id 
	INNER JOIN fp.dim_cities dc ON dc.id = b.city_id 
	GROUP BY dt.calendar_year, dc.city, dc.country
	HAVING sum(b.births_count) IS NOT NULL 
	WINDOW w AS (PARTITION BY DC.city ORDER BY dt.calendar_year ASC)
	) AS tab
WHERE tab.calendar_year = DATE_PART('year', CURRENT_DATE)-1 
ORDER BY tab.current_births DESC
LIMIT 10;

-- View for tracking the dynamics of average wages over the last 5 years
CREATE OR REPLACE VIEW fp.salary_analyse
AS 
SELECT 
	foo.time_id,
	foo.city_ascii,
	foo.percent_avg_salary
FROM (
	SELECT 
		tab.*,
		round(avg_salary*100/(LAG(avg_salary, 1) OVER (PARTITION BY tab.city_ascii, tab.country ORDER BY tab.time_id ASC)), 2)-100 AS percent_avg_salary
	FROM (
		SELECT DISTINCT
			dt.time_id,
			dt.calendar_year,
			dc.city_ascii,
			dc.country,
			avg(e.ordinary_time_weekly + e.overtime_weekly) OVER w AS avg_salary
		FROM fp.earnings e 
		INNER JOIN fp.dim_times dt ON dt.time_id = e.time_id 
		INNER JOIN fp.dim_cities dc ON dc.id = e.city_id 
		WINDOW w AS (PARTITION BY dt.time_id, dc.city_ascii, dc.country)
		) AS tab
	) AS foo
WHERE foo.calendar_year <= DATE_PART('year', CURRENT_DATE)-1 
AND foo.calendar_year >= DATE_PART('year', CURRENT_DATE)-5;

CREATE OR REPLACE VIEW fp.population_kindergarten_analyse
AS 
SELECT dc.city_ascii, dc.admin_name, dc.country, p.population, count (k.*)
FROM fp.population p 
INNER JOIN fp.dim_cities dc ON dc.id = p.city_id 
INNER JOIN fp.kindergartens k ON k.city_id = dc.id 
WHERE dc.country = 'New Zealand'
GROUP BY dc.city_ascii, dc.admin_name, dc.country, p.population;


-- View for analyzing the ratio of the number of children born in the last 5 years by city and the number of children in kindergartens in this city
CREATE OR REPLACE VIEW fp.analysis_of_free_places
AS
SELECT 
	dc.city_ascii,
	dc.admin_name,
	dc.country,
	sum(b.births_count) AS childs_born,
	sum(k."All_Children") AS max_studying,
	round(sum(k."All_Children")*100/sum(b.births_count)::NUMERIC(18,2), 2) child_perc_can_go_to_kindergarten
FROM fp.births b 
INNER JOIN fp.dim_times dt ON dt.time_id = b.time_id 
INNER JOIN fp.dim_cities dc ON dc.id = b.city_id 
INNER JOIN fp.kindergartens k ON k.city_id = dc.id 
WHERE dt.calendar_year <= DATE_PART('year', CURRENT_DATE)-1 
AND dt.calendar_year >= DATE_PART('year', CURRENT_DATE)-5
GROUP BY dc.city_ascii, dc.admin_name, dc.country
HAVING sum(b.births_count) IS NOT NULL;
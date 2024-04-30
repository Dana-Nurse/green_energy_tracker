-- Create Locations table
CREATE TABLE Locations (
    location_id INT PRIMARY KEY,
    location_name VARCHAR(100),
    country VARCHAR(100),
    region VARCHAR(100)
);

-- Create EnergySources table
CREATE TABLE EnergySources (
    source_id INT PRIMARY KEY,
    source_name VARCHAR(100),
    commentary VARCHAR(255),
    capacity DECIMAL(10, 2)
);

-- Create EnergyProduction table
CREATE TABLE EnergyProduction (
    production_id INT PRIMARY KEY,
    source_id INT,
    location_id INT,
    production_date DATE,
    production_value DECIMAL(12, 2),
    FOREIGN KEY (source_id) REFERENCES EnergySources(source_id),
    FOREIGN KEY (location_id) REFERENCES Locations(location_id)
);

-- Create EnvironmentalImpact table
CREATE TABLE EnvironmentalImpact (
    impact_id INT PRIMARY KEY,
    production_id INT,
    CO2_emissions DECIMAL(12, 2),
    water_usage DECIMAL(12, 2),
    land_use DECIMAL(12, 2),
    FOREIGN KEY (production_id) REFERENCES EnergyProduction(production_id)
);

-- Create Users table
CREATE TABLE Users (
    user_id INT PRIMARY KEY,
    username VARCHAR(100),
    email VARCHAR(255),
    password VARCHAR(255)
);


-- 3. Create a view that combines multiple tables in a logical way
CREATE VIEW EnergyProductionSummary AS
SELECT EP.production_id, EP.production_date, EP.production_value,
    ES.source_name, ES.commentary, ES.capacity,
    L.location_name, L.country, L.region
FROM EnergyProduction EP
INNER JOIN EnergySources ES ON EP.source_id = ES.source_id
INNER JOIN Locations L ON EP.location_id = L.location_id;


-- 4. A stored function that calculates the total CO2 Emissions 
DELIMITER $$
CREATE FUNCTION CO2_emissions_for_location(location_id INT)
RETURNS DECIMAL(12, 2)
BEGIN
    DECLARE CO2_emissions DECIMAL(12, 2);
    -- Calculation to sum the CO2 emissions for the selected location
    SELECT SUM(CO2_emissions) INTO CO2_emissions
    FROM EnvironmentalImpact
    WHERE production_id IN (SELECT production_id FROM EnergyProduction WHERE location_id = location_id);
    RETURN CO2_emissions;
END $$
DELIMITER ;


-- 5. A query with a subquery 'having' to demonstrate how to extract data for data analysis 
SELECT L.location_id, L.location_name
FROM Locations L
LEFT JOIN EnergyProduction EP ON L.location_id = EP.location_id
GROUP BY L.location_id, L.location_name
HAVING COUNT(EP.production_id) = 0;


-- AVDANCED PROJECT REQUIREMENTS

-- 1. This is a stored procedure that gets the locations with the highest energy production 
CREATE PROCEDURE GetLocationsWithHighestEnergy(
    IN max_locations INT
) 

BEGIN 
    SELECT 
        L.location_id, 
        L.location_name, 
        SUM(EP.production_value) AS total_production -- Total production value for the location
    FROM 
        Locations L
    INNER JOIN 
        EnergyProduction EP ON L.location_id = EP.location_id -- this joins the 'Locations' and 'EnergyProduction' tables
    GROUP BY 
        L.location_id, L.location_name -- this groups the results by location ID and name
    ORDER BY 
        total_production DESC 
    LIMIT 
        max_locations; -- uses the limit keywork to limit  the number of locations returned by the specific input
END;


-- 2. An example of a trigger 
CREATE TRIGGER EnvironmentalImpactIncreaseAlert
AFTER INSERT ON EnvironmentalImpact
FOR EACH ROW
BEGIN
    -- this checks if CO2 emissions have increased
    IF NEW.CO2_emissions > 0 THEN
        -- Print message to demonstrate increase in CO2 emissions
        SELECT 'Increase in CO2 emissions detected';
    END IF;
    
    -- this checks if water usage has increased
    IF NEW.water_usage > 0 THEN
        -- Print message to demonstrate increase in water usage
        SELECT 'Increase in water usage detected';
    END IF;
    
    -- this checks if land use has increased
    IF NEW.land_use > 0 THEN
        -- Print message to demonstrate increase in land use
        SELECT 'Increase in land use detected';
    END IF;
END;


-- 3. An example of an event used to keep data becoming excessively long 
CREATE EVENT CleanedEnergyProductionData
ON SCHEDULE EVERY 2 WEEK
DO
BEGIN
    DELETE FROM EnergyProduction WHERE production_date < DATE_SUB(NOW(), INTERVAL 1 YEAR);
END;


